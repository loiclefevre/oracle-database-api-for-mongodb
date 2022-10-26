package com.oracle.example.mongodbapi.racing;

import com.mongodb.AuthenticationMechanism;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.connection.ClusterConnectionMode;
import org.bson.Document;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.OracleContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
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

public class RacingTest {
	private static final Logger LOG = LoggerFactory.getLogger(RacingTest.class);

	// Oracle Database XE 21c container
	static OracleContainer oracleXE21c;

	// Oracle REST Data Services 22.3.1 container with MongoDB Compatible API
	static GenericContainer ords;

	// Shared network across containers
	static Network network;

	private static final String DATABASE_USER = "test";
	private static final String DATABASE_PASSWORD = "test";

	@BeforeAll
	public static void startDatabase() {
		oracleXE21c = new OracleContainer("gvenzl/oracle-xe:21.3.0-slim")
				//.withImagePullPolicy(PullPolicy.ageBased(Duration.ofDays(1)))
				.withReuse(true)
				.withNetworkAliases("database")
				.withExposedPorts(1521, 27017) // expose port of other containers in same network namespace
				.withEnv("ORACLE_PASSWORD", "test")
				.withEnv("APP_USER_PASSWORD", DATABASE_PASSWORD)
				.withEnv("APP_USER", DATABASE_USER);
		oracleXE21c.start();

		ords = new GenericContainer<>(DockerImageName.parse("loiclefevre/ords-ol8:latest"))
				.dependsOn(oracleXE21c)
				//.withImagePullPolicy(PullPolicy.alwaysPull())
				.withReuse(true)
				.withNetworkMode("container:" + oracleXE21c.getContainerId())
				.withLogConsumer(new Slf4jLogConsumer(LOG))
				.withEnv("DB_HOSTNAME", "localhost") // other containers in same network namespace are reachable via localhost
				.withEnv("DB_SERVICE", "xepdb1")
				.withEnv("DB_PORT", "1521")
				.withEnv("SYS_PASSWORD", "test")
				.waitingFor(Wait.forLogMessage(".*Oracle REST Data Services initialized.*\\s+", 1));
		ords.start();

		// finish MongoDB API installation for test user
		try (Connection c = DriverManager.getConnection("jdbc:oracle:thin:@//localhost:" + oracleXE21c.getOraclePort() + "/xepdb1", "SYSTEM", "test")) {
			try (Statement s = c.createStatement()) {
				s.execute("grant soda_app, create session, create table, create view, create sequence, create procedure, create job, unlimited tablespace to test");
			}
		}
		catch (SQLException sqle) {
			throw new IllegalStateException(sqle);
		}

		// finish MongoDB API installation for test user
		try (Connection c = DriverManager.getConnection("jdbc:oracle:thin:@//localhost:" + oracleXE21c.getOraclePort() + "/xepdb1", DATABASE_USER, DATABASE_PASSWORD)) {
			try (CallableStatement cs = c.prepareCall("{call ORDS.ENABLE_SCHEMA()}")) {
				cs.execute();
			}
		}
		catch (SQLException sqle) {
			throw new IllegalStateException(sqle);
		}
	}

	@AfterAll
	public static void stopDatabase() {
		LOG.info("Stopping ORDS and database...");
		//ords.stop();
		//oracleXE21c.stop();
	}

	@Test
	public void testDisplayVersions() {
		try {
			//System.setProperty("https.protocols", "TLSv1.3");

			final MongoCredential credential = MongoCredential.createCredential(DATABASE_USER, "$external", DATABASE_PASSWORD.toCharArray());

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

			final MongoClientSettings settings = MongoClientSettings.builder()
					.applyToSocketSettings(builder -> builder.connectTimeout(1, TimeUnit.DAYS))
					.credential(credential.withMechanism(AuthenticationMechanism.PLAIN))
					.applyToConnectionPoolSettings(builder -> builder.maxSize(5).minSize(1).maxConnecting(5).maxConnectionIdleTime(10, TimeUnit.MINUTES)
							//.maintenanceInitialDelay(5, TimeUnit.MINUTES)
							//.maintenanceFrequency(5,TimeUnit.MINUTES)
					)
					.applyToClusterSettings(builder -> builder.mode(ClusterConnectionMode.LOAD_BALANCED).hosts(List.of(new ServerAddress("localhost", oracleXE21c.getMappedPort(27017)))))
					.applyToSslSettings(builder -> {
						builder.enabled(true);
						builder.context(sslContext);
					})
					.retryWrites(false)
					.build();

			try (MongoClient mongoClient = MongoClients.create(settings)) {
				MongoDatabase mongoDatabase = mongoClient.getDatabase(DATABASE_USER);
				final Document result = mongoDatabase.runCommand(new Document("buildInfo", 1));
				final String mongoDBVersion = (String) result.get("version");

				LOG.info("MongoDB version: {}", mongoDBVersion);

				Assertions.assertEquals(mongoDBVersion, "4.2.14");
			}
		}
		catch (NoSuchAlgorithmException | KeyManagementException e) {
			Assertions.fail("Can't connect to the Oracle Database API for MongoDB database!");
		}
	}
}

