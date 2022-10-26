package com.oracle.example.mongodbapi.racing;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.ORDSMongoDBAPIContainer;

public class Racing2Test {
	private static final Logger LOG = LoggerFactory.getLogger(Racing2Test.class);

	static ORDSMongoDBAPIContainer mongoDBAPIContainer;

	private static final String DATABASE_USER = "test";
	private static final String DATABASE_PASSWORD = "test";

	@BeforeAll
	public static void startDatabase() {
		mongoDBAPIContainer = new ORDSMongoDBAPIContainer(DATABASE_USER, DATABASE_PASSWORD);
		mongoDBAPIContainer.start();
	}

	@Test
	public void testDisplayVersions() {
		try (MongoClient mongoClient = MongoClients.create(mongoDBAPIContainer.getMongoClientSettings(1, 5))) {
			MongoDatabase mongoDatabase = mongoClient.getDatabase(DATABASE_USER);
			final Document result = mongoDatabase.runCommand(new Document("buildInfo", 1));
			final String mongoDBVersion = (String) result.get("version");

			LOG.info("MongoDB version: {}", mongoDBVersion);

			Assertions.assertEquals(mongoDBVersion, "4.2.14");
		}
	}
}
