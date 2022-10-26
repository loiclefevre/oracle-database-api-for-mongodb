package org.testcontainers.containers;

import com.mongodb.AuthenticationMechanism;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.connection.ClusterConnectionMode;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.X509Certificate;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class ORDSMongoDBAPIContainer extends GenericContainer<ORDSMongoDBAPIContainer> {

	private static final int MONGODB_PORT = 27017;

	protected OracleContainer databaseContainer;
	protected String user;
	protected String password;

	public ORDSMongoDBAPIContainer(String user, String password) {
		this(DockerImageName.parse("loiclefevre/ords-ol8:latest"));
		this.user = user;
		this.password = password;
		this.databaseContainer = new OracleContainer("gvenzl/oracle-xe:21.3.0-slim")
				//.withImagePullPolicy(PullPolicy.ageBased(Duration.ofDays(1)))
				.withReuse(true)
				.withNetworkAliases("database")
				.withExposedPorts(1521, MONGODB_PORT) // expose port of other containers in same network namespace
				.withEnv("ORACLE_PASSWORD", password)
				.withEnv("APP_USER", user)
				.withEnv("APP_USER_PASSWORD", password);
		this.dependsOn(databaseContainer);
	}

	protected ORDSMongoDBAPIContainer(DockerImageName dockerImageName) {
		super(dockerImageName);
	}


	@Override
	protected void configure() {
		super.configure();

		databaseContainer.start();

		this.withReuse(true)
				//.withLogConsumer(new Slf4jLogConsumer(LoggerFactory.getLogger(ORDSMongoDBAPIContainer.class)))
				.withNetworkMode("container:" + databaseContainer.getContainerId())
				.withEnv("DB_HOSTNAME", "localhost") // other containers in same network namespace are reachable via localhost
				.withEnv("DB_SERVICE", databaseContainer.getDatabaseName())
				.withEnv("DB_PORT", "1521")
				.withEnv("SYS_PASSWORD", password)
				.waitingFor(Wait.forLogMessage(".*Oracle REST Data Services initialized.*\\s+", 1));
	}

	@Override
	protected void doStart() {
		super.doStart();

		// finish MongoDB API installation for user
		try (Connection c = DriverManager.getConnection(String.format("jdbc:oracle:thin:@//localhost:%d/%s", databaseContainer.getOraclePort(), databaseContainer.getDatabaseName()), "SYSTEM", password)) {
			try (Statement s = c.createStatement()) {
				s.execute(String.format("grant soda_app, create session, create table, create view, create sequence, create procedure, create job, unlimited tablespace to %s", user));
			}
		}
		catch (SQLException sqle) {
			throw new IllegalStateException(sqle);
		}

		// finish MongoDB API installation for user
		try (Connection c = DriverManager.getConnection(String.format("jdbc:oracle:thin:@//localhost:%d/%s", databaseContainer.getOraclePort(), databaseContainer.getDatabaseName()), user, password)) {
			try (CallableStatement cs = c.prepareCall("{call ORDS.ENABLE_SCHEMA()}")) {
				cs.execute();
			}
		}
		catch (SQLException sqle) {
			throw new IllegalStateException(sqle);
		}
	}

	@Override
	public void stop() {
		super.stop();

		databaseContainer.stop();
	}

	protected MongoClientSettings mongoClientSettings;

	public MongoClientSettings getMongoClientSettings(int minPoolSize, int maxPoolSize) {
		if (mongoClientSettings == null) {
			try {
				final MongoCredential credential = MongoCredential.createCredential(user, "$external", password.toCharArray());

				final TrustManager[] trustAllCerts = new TrustManager[]{new X509TrustManager() {
					public java.security.cert.X509Certificate[] getAcceptedIssuers() {
						return null;
					}

					public void checkClientTrusted(X509Certificate[] certs, String authType) {
					}

					public void checkServerTrusted(X509Certificate[] certs, String authType) {
					}
				}
				};

				final SSLContext sslContext = SSLContext.getInstance("SSL");
				sslContext.init(null, trustAllCerts, new java.security.SecureRandom());

				mongoClientSettings = MongoClientSettings.builder()
						.applyToSocketSettings(builder -> builder.connectTimeout(1, TimeUnit.DAYS))
						.credential(credential.withMechanism(AuthenticationMechanism.PLAIN))
						.applyToConnectionPoolSettings(builder -> builder.maxSize(maxPoolSize)
										.minSize(minPoolSize)
										.maxConnecting(Runtime.getRuntime().availableProcessors())
										.maxConnectionIdleTime(2, TimeUnit.MINUTES)
								//.maintenanceInitialDelay(5, TimeUnit.MINUTES)
								//.maintenanceFrequency(5,TimeUnit.MINUTES)
						)
						.applyToClusterSettings(builder -> builder.mode(ClusterConnectionMode.LOAD_BALANCED).hosts(List.of(new ServerAddress("localhost", databaseContainer.getMappedPort(MONGODB_PORT)))))
						.applyToSslSettings(builder -> {
							builder.enabled(true);
							builder.context(sslContext);
						})
						.retryWrites(false)
						.build();
			}
			catch (NoSuchAlgorithmException | KeyManagementException e) {
				throw new RuntimeException("Problem initializing MongoDB Client Settings for connection!", e);
			}
		}

		return mongoClientSettings;
	}
}
