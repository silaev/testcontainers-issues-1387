package com.testcontainers.issues_1387;

import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.TransactionBody;
import org.bson.Document;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;

import java.io.IOException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * @author Konstantin Silaev on 10/8/2019
 */
class Test1387 {
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(Test1387.class);
    private static Network network = Network.newNetwork();
    private static int MONGO_PORT = 27017;
    public static GenericContainer mongo1 = new GenericContainer("mongo:4.0.8")
        .withNetwork(network)
        .withNetworkAliases("M1")
        .withExposedPorts(MONGO_PORT)
        .withCommand("--replSet rs0 --bind_ip localhost,M1");

    public static GenericContainer mongo2 = new GenericContainer("mongo:4.0.8")
        .withNetwork(network)
        .withNetworkAliases("M2")
        .withExposedPorts(MONGO_PORT)
        .withCommand("--replSet rs0 --bind_ip localhost,M2");

    public static GenericContainer mongo3 = new GenericContainer("mongo:4.0.8")
        .withNetwork(network)
        .withNetworkAliases("M3")
        .withExposedPorts(MONGO_PORT)
        .withCommand("--replSet rs0 --bind_ip localhost,M3");

    @BeforeAll
    static void init() throws IOException, InterruptedException {
        mongo1.start();
        mongo2.start();
        mongo3.start();

        mongo1.execInContainer("/bin/bash", "-c", "mongo --eval 'printjson(rs.initiate({_id:\"rs0\"," +
            "members:[{_id:0,host:\"M1:27017\"},{_id:1,host:\"M2:27017\"},{_id:2,host:\"M3:27017\"}]}))' --quiet");
        mongo1.execInContainer("/bin/bash", "-c",
            "until mongo --eval \"printjson(rs.isMaster())\" | grep ismaster | grep true > /dev/null 2>&1;do sleep 1;done");
    }

    @Test
    void shouldExecuteTransactionsReplicaSet() {
        String mongoRsUrl = buildMongoRsUrl(mongo1, mongo2, mongo3);
        assertNotNull(mongoRsUrl);

        execTransaction(mongoRsUrl);
    }

    @Test
    void shouldExecuteTransactionsStandAlone() {
        execTransaction(buildMongoStandAloneUrl());
    }

    private void execTransaction(final String mongoRsUrl) {
        //GIVEN

        MongoClient mongoSyncClient = MongoClients.create(mongoRsUrl);
        mongoSyncClient.getDatabase("mydb1").getCollection("foo")
            .withWriteConcern(WriteConcern.MAJORITY).insertOne(new Document("abc", 0));
        mongoSyncClient.getDatabase("mydb2").getCollection("bar")
            .withWriteConcern(WriteConcern.MAJORITY).insertOne(new Document("xyz", 0));

        ClientSession clientSession = mongoSyncClient.startSession();
        TransactionOptions txnOptions = TransactionOptions.builder()
            .readPreference(ReadPreference.primary())
            .readConcern(ReadConcern.LOCAL)
            .writeConcern(WriteConcern.MAJORITY)
            .build();

        String trxResult = "Inserted into collections in different databases";

        //WHEN + THEN
        TransactionBody<String> txnBody = () -> {
            MongoCollection<Document> coll1 = mongoSyncClient.getDatabase("mydb1").getCollection("foo");
            MongoCollection<Document> coll2 = mongoSyncClient.getDatabase("mydb2").getCollection("bar");

            coll1.insertOne(clientSession, new Document("abc", 1));
            coll2.insertOne(clientSession, new Document("xyz", 999));
            return trxResult;
        };

        try {
            String trxResultActual = clientSession.withTransaction(txnBody, txnOptions);
            assertEquals(trxResult, trxResultActual);
        } catch (RuntimeException re) {
            assertEquals(
                "Something went wrong during exec a transaction",
                re.getMessage()
            );
        } finally {
            clientSession.close();
            mongoSyncClient.close();
        }
    }

    private String buildMongoRsUrl(GenericContainer... mongos) {
        String hosts = Stream.of(mongos)
            .map(c -> c.getContainerIpAddress() + ":" + c.getMappedPort(MONGO_PORT))
            .collect(Collectors.joining(","));

        String url = String.format(
            "mongodb://%s/test?replicaSet=rs0", hosts
        );
        log.debug("url: {}", url);
        return url;
    }

    private String buildMongoStandAloneUrl() {
        String url = String.format(
            "mongodb://localhost:%d/test", mongo1.getMappedPort(MONGO_PORT)
        );
        log.debug("url: {}", url);
        return url;
    }
}
