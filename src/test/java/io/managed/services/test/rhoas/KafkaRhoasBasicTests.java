package io.managed.services.test.rhoas;

import com.openshift.cloud.api.kas.auth.models.Record;
import com.openshift.cloud.api.kas.auth.models.Topic;
import com.openshift.cloud.api.kas.models.KafkaRequest;
import com.openshift.cloud.api.serviceaccounts.models.ServiceAccountData;
import io.managed.services.test.Environment;
import io.managed.services.test.TestBase;
import io.managed.services.test.cli.CLI;
import io.managed.services.test.cli.CLIDownloader;
import io.managed.services.test.cli.CLIUtils;
import io.managed.services.test.cli.CliGenericException;
import io.managed.services.test.cli.CliNotFoundException;
import io.managed.services.test.cli.ServiceAccountSecret;
import io.managed.services.test.cli.CLI.ACLEntityType;
import io.managed.services.test.client.kafkainstance.KafkaInstanceApiUtils;
import io.managed.services.test.client.kafkamgmt.KafkaMgmtApiUtils;
import io.managed.services.test.client.securitymgmt.SecurityMgmtAPIUtils;
import io.vertx.core.Vertx;
import lombok.SneakyThrows;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.util.List;
import java.util.Objects;

import static io.managed.services.test.TestUtils.bwait;
import static io.managed.services.test.client.kafka.KafkaMessagingUtils.testTopic;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


/**
 * Test the application services CLI[1] kafka commands.
 * <p>
 * The tests download the CLI from GitHub to the local machine where the test suite is running
 * and perform all operations using the CLI.
 * <p>
 * By default the latest version of the CLI is downloaded otherwise a specific version can be set using
 * the CLI_VERSION env. The CLI platform (linux, mac, win) and arch (amd64, arm) is automatically detected,
 * or it can be enforced using the CLI_PLATFORM and CLI_ARCH env.
 * <p>
 * 1. https://github.com/redhat-developer/app-services-cli
 * <p>
 * <b>Requires:</b>
 * <ul>
 *     <li> PRIMARY_OFFLINE_TOKEN
 *     <li> PRIMARY_USERNAME
 *     <li> PRIMARY_PASSWORD
 * </ul>
 */
@Test
public class KafkaRhoasBasicTests extends TestBase {
    private static final Logger LOGGER = LogManager.getLogger(KafkaRhoasBasicTests.class);

    private static final String KAFKA_INSTANCE_NAME = "e2e-cli-test-" + Environment.LAUNCH_SUFFIX;
    private static final String SERVICE_ACCOUNT_NAME = "e2e-cli-svc-acc-"  + Environment.LAUNCH_SUFFIX;
    private static final String TOPIC_NAME = "e2e-cli-topic" + Environment.LAUNCH_SUFFIX;
    // used for testing quickstart for data production and consumption
    private static final String TOPIC_NAME_PRODUCE_CONSUME = "produce-consume-test-topic" + Environment.LAUNCH_SUFFIX;
    private static final int DEFAULT_PARTITIONS = 1;
    private static final String CONSUMER_GROUP_NAME = "consumer-group-1";

    private final Vertx vertx = Vertx.vertx();

    private CLI cli;

    private KafkaRequest kafka;
    private ServiceAccountSecret serviceAccountSecret;
    private ServiceAccountData serviceAccount;
    private Topic topic;

    private final List<String> records = List.of("First message", "Second message", "Third message");
    private final String customRecordKey = "my-key";

    @BeforeClass
    public void bootstrap() {
        assertNotNull(Environment.PRIMARY_USERNAME, "the PRIMARY_USERNAME env is null");
        assertNotNull(Environment.PRIMARY_PASSWORD, "the PRIMARY_PASSWORD env is null");
        assertNotNull(Environment.PRIMARY_OFFLINE_TOKEN, "the PRIMARY_OFFLINE_TOKEN env is null");
    }

    @AfterClass(alwaysRun = true)
    @SneakyThrows
    public void clean() {

        var offlineToken = Environment.PRIMARY_OFFLINE_TOKEN;

        var kafkaMgmtApi =  KafkaMgmtApiUtils.kafkaMgmtApi(Environment.OPENSHIFT_API_URI, offlineToken);
        var securityMgmtApi = SecurityMgmtAPIUtils.securityMgmtApi(Environment.OPENSHIFT_API_URI, offlineToken);

        if (Environment.SKIP_KAFKA_TEARDOWN) {
            LOGGER.warn("skip kafka instance clean up");
            return;
        }
        
        try {
            KafkaMgmtApiUtils.deleteKafkaByNameIfExists(kafkaMgmtApi, KAFKA_INSTANCE_NAME);
        } catch (Throwable t) {
            LOGGER.error("delete kafka instance error: ", t);
        }

        try {
            SecurityMgmtAPIUtils.cleanServiceAccount(securityMgmtApi, SERVICE_ACCOUNT_NAME);
        } catch (Throwable t) {
            LOGGER.error("delete service account error: ", t);
        }

        // delete topic used in quickstart
        try {
            cli.deleteTopic(TOPIC_NAME_PRODUCE_CONSUME);
        } catch (Throwable t) {
            LOGGER.error("delete topic error: ", t);
        }

        try {
            LOGGER.info("logout user from rhoas");
            cli.logout();
        } catch (Throwable t) {
            LOGGER.error("CLI logout error: ", t);
        }

        try {
            LOGGER.info("delete workdir: {}", cli.getWorkdir());
            FileUtils.deleteDirectory(new File(cli.getWorkdir()));
        } catch (Throwable t) {
            LOGGER.error("clean workdir error: ", t);
        }

        bwait(vertx.close());
    }

    @Test
    @SneakyThrows
    public void testDownloadCLI() {

        var downloader = CLIDownloader.defaultDownloader();

        // download the cli
        var binary = downloader.downloadCLIInTempDir();

        this.cli = new CLI(binary);

        LOGGER.info("validate cli");
        LOGGER.debug(cli.help());
    }


    @Test(dependsOnMethods = "testDownloadCLI", enabled = true)
    @SneakyThrows
    public void testLogin() {

        // make sure you are logout while developing locally.
        LOGGER.info("verify that we aren't logged-in");
        assertThrows(CliGenericException.class, () -> cli.listKafka());

        LOGGER.info("login the CLI");
        CLIUtils.login(vertx, cli, Environment.PRIMARY_USERNAME, Environment.PRIMARY_PASSWORD).get();

        LOGGER.info("verify that we are logged-in");
        cli.listKafka();
    }

    @Test(dependsOnMethods = "testLogin", enabled = true)
    @SneakyThrows
    public void testCreateServiceAccount() {

        LOGGER.info("create a service account");
        serviceAccountSecret = CLIUtils.createServiceAccount(cli, SERVICE_ACCOUNT_NAME);

        LOGGER.info("get the service account");
        var sa = CLIUtils.getServiceAccountByName(cli, SERVICE_ACCOUNT_NAME);
        LOGGER.debug(sa);

        assertTrue(sa.isPresent());
        assertEquals(sa.get().getName(), SERVICE_ACCOUNT_NAME);
        assertEquals(sa.get().getClientId(), serviceAccountSecret.getClientID());

        serviceAccount = sa.get();
    }

    @Test(dependsOnMethods = "testCreateServiceAccount", enabled = true)
    @SneakyThrows
    public void testDescribeServiceAccount() {

        LOGGER.info("describe service account by id field");
        var sa = cli.describeServiceAccount(serviceAccount.getId());
        LOGGER.debug(sa);

        assertEquals(sa.getName(), SERVICE_ACCOUNT_NAME);
    }

    @Test(dependsOnMethods = "testLogin", enabled = true)
    @SneakyThrows
    public void testCreateKafkaInstance() {

        LOGGER.info("create kafka instance with name {}", KAFKA_INSTANCE_NAME);
        var k = cli.createKafka(KAFKA_INSTANCE_NAME);
        LOGGER.debug(k);

        LOGGER.info("wait for kafka instance: {}", k.getId());
        kafka = CLIUtils.waitUntilKafkaIsReady(cli, k.getId());
        LOGGER.debug(kafka);
    }

    @Test(dependsOnMethods = {"testCreateKafkaInstance", "testCreateServiceAccount"}, enabled = true)
    @SneakyThrows
    public void testGrantProducerAndConsumerAccess() {
        LOGGER.info("grant producer and consumer access to the account: {}", serviceAccount.getClientId());
        cli.grantAccessAcl(ACLEntityType.SERVICE_ACCOUNT, serviceAccount.getClientId(), "all", "all");

        var acl = cli.listACLs();
        LOGGER.debug(acl);
    }

    @Test(dependsOnMethods = {"testCreateKafkaInstance", "testGrantProducerAndConsumerAccess"}, enabled = true)
    @SneakyThrows
    public void testProducePlainMessages() {
        LOGGER.info("create topic '{}' with 2 partitions and other default configuration", TOPIC_NAME_PRODUCE_CONSUME);
        CLIUtils.applyTopic(cli, TOPIC_NAME_PRODUCE_CONSUME, 2);

        var messages = List.of("First message", "Second message", "Third message");

        LOGGER.info("produce messages");
        int i = 0;
        for (String message: messages) {
            // produce single message
            LOGGER.info("Produce message '{}'", message);
            Record record =  cli.produceRecords(TOPIC_NAME_PRODUCE_CONSUME, kafka.getId(), message);
            LOGGER.debug(record);

            assertEquals(record.getValue(), message);
            assertEquals(record.getOffset(), i++);
            // if no partition is provided expected value is 0
            assertEquals(record.getPartition(), 0);
        }
    }

    @Test(dependsOnMethods = {"testCreateKafkaInstance", "testGrantProducerAndConsumerAccess"}, enabled = true)
    @SneakyThrows
    public void testProduceCustomMessage() {
        LOGGER.info("create topic '{}' with 2 partitions and other default configuration", TOPIC_NAME_PRODUCE_CONSUME);
        CLIUtils.applyTopic(cli, TOPIC_NAME_PRODUCE_CONSUME, 2);

        Record record =  cli.produceRecords(TOPIC_NAME_PRODUCE_CONSUME, kafka.getId(), "someValue", 1, customRecordKey);
        LOGGER.debug(record);

        assertEquals(record.getPartition(), 1);
        assertEquals(record.getKey(), customRecordKey);
    }

    @Test(dependsOnMethods = {"testProducePlainMessages"}, enabled = true)
    @SneakyThrows
    public void testConsumeMessages() {
        LOGGER.info("consuming all messages from partition '0' topic '{}'", TOPIC_NAME_PRODUCE_CONSUME);
        List<Record> consumedRecords = cli.consumeRecords(TOPIC_NAME_PRODUCE_CONSUME, kafka.getId(), 0, 0);

        int i = 0;
        for (Record consumedRecord: consumedRecords) {
            LOGGER.debug(consumedRecord);
            assertEquals(consumedRecord.getPartition(), 0, "failed to read partition 0");
            assertEquals(consumedRecord.getOffset(), i, "failed to obtain expected offset");
            assertEquals(consumedRecord.getValue(), this.records.get(i), "failed to obtain expected message");
            i++;
        }
    }

    @Test(dependsOnMethods = {"testProducePlainMessages"}, enabled = true)
    @SneakyThrows
    public void testConsumeSpecificOffset() {
        LOGGER.info("consuming all messages from partition '0' topic '{}'", TOPIC_NAME_PRODUCE_CONSUME);
        List<Record> consumedRecords = cli.consumeRecords(TOPIC_NAME_PRODUCE_CONSUME, kafka.getId(), 0, 2);

        // get the only obtained record
        Record consumedRecord = consumedRecords.get(0);
        LOGGER.debug(consumedRecord);
        assertEquals(consumedRecord.getPartition(), 0, "failed to read partition 0");
        assertEquals(consumedRecord.getOffset(), 2, "failed to obtain expected offset");
        assertEquals(consumedRecord.getValue(), records.get(2), "failed to obtain expected message");
    }

    @Test(dependsOnMethods = {"testProduceCustomMessage"}, enabled = true)
    @SneakyThrows
    public void testConsumeCustomMessage() {
        LOGGER.info("consuming all messages from partition '1' topic '{}'", TOPIC_NAME_PRODUCE_CONSUME);
        List<Record> consumedRecords = cli.consumeRecords(TOPIC_NAME_PRODUCE_CONSUME, kafka.getId(), 1);

        Record consumedRecord = consumedRecords.get(0);
        LOGGER.debug(consumedRecord);
        assertEquals(consumedRecord.getPartition(), 1, "failed to read partition 1");
        assertEquals(consumedRecord.getKey(), customRecordKey, "failed to obtain expected key");
    }

    @Test(dependsOnMethods = "testCreateKafkaInstance", enabled = true)
    @SneakyThrows
    public void testDescribeKafkaInstance() {

        LOGGER.info("get kafka instance with name {}", KAFKA_INSTANCE_NAME);
        var k = cli.describeKafka(kafka.getId());
        LOGGER.debug(k);

        assertEquals("ready", k.getStatus());
    }

    @Test(dependsOnMethods = "testCreateKafkaInstance", enabled = true)
    @SneakyThrows
    public void testListKafkaInstances() {

        var list = cli.listKafka();
        LOGGER.debug(list);

        var exists = list.getItems().stream()
            .filter(k -> KAFKA_INSTANCE_NAME.equals(k.getName()))
            .findAny();
        assertTrue(exists.isPresent());
    }

    @Test(dependsOnMethods = "testCreateKafkaInstance", enabled = true)
    @SneakyThrows
    public void testSearchKafkaByName() {

        var list = cli.searchKafkaByName(KAFKA_INSTANCE_NAME);
        LOGGER.debug(list);

        var exists = list.getItems().stream().findAny();
        assertTrue(exists.isPresent());
        assertEquals(exists.get().getName(), KAFKA_INSTANCE_NAME);
    }

    @Test(dependsOnMethods = "testCreateKafkaInstance", enabled = true)
    @SneakyThrows
    public void testCreateTopic() {

        LOGGER.info("create kafka topic with name {}", KAFKA_INSTANCE_NAME);
        topic = cli.createTopic(TOPIC_NAME);
        LOGGER.debug(topic);

        assertEquals(topic.getName(), TOPIC_NAME);
        assertEquals(Objects.requireNonNull(topic.getPartitions()).size(), DEFAULT_PARTITIONS);
    }

    @Test(dependsOnMethods = "testCreateTopic", enabled = true)
    @SneakyThrows
    public void testListTopics() {

        var list = cli.listTopics();
        LOGGER.debug(list);

        var exists = Objects.requireNonNull(list.getItems()).stream()
            .filter(t -> TOPIC_NAME.equals(t.getName()))
            .findAny();
        assertTrue(exists.isPresent());
    }


    @Test(dependsOnMethods = {"testCreateTopic", "testGrantProducerAndConsumerAccess"}, enabled = true)
    @SneakyThrows
    public void testKafkaInstanceTopic() {

        var bootstrapHost = kafka.getBootstrapServerHost();
        var clientID = serviceAccountSecret.getClientID();
        var clientSecret = serviceAccountSecret.getClientSecret();

        bwait(testTopic(
            vertx,
            bootstrapHost,
            clientID,
            clientSecret,
            TOPIC_NAME,
            1000,
            10,
            100));
    }

    @Test(dependsOnMethods = "testCreateTopic", enabled = true)
    @SneakyThrows
    public void testUpdateTopic() {

        var retentionTime = "4";
        var retentionKey = "retention.ms";

        LOGGER.info("update kafka topic with name {}", TOPIC_NAME);
        cli.updateTopic(TOPIC_NAME, retentionTime);
        var t = cli.describeTopic(TOPIC_NAME);
        LOGGER.debug(t);

        var retentionValue = Objects.requireNonNull(t.getConfig())
            .stream()
            .filter(conf -> retentionKey.equals(conf.getKey()))
            .findFirst();

        assertTrue(retentionValue.isPresent(), "updated config not found");
        assertEquals(retentionValue.get().getValue(), retentionTime);

        topic = t;
    }

    @Test(dependsOnMethods = "testUpdateTopic", enabled = true)
    @SneakyThrows
    public void testDescribeUpdatedTopic() {

        var retentionTime = "4";
        var retentionKey = "retention.ms";

        LOGGER.info("describe kafka topic with name {}", TOPIC_NAME);
        var t = cli.describeTopic(TOPIC_NAME);
        LOGGER.debug(t);

        assertEquals(t.getName(), TOPIC_NAME);
        assertEquals(
            Objects.requireNonNull(t.getPartitions()).size(),
            Objects.requireNonNull(topic.getPartitions()).size());

        var retentionValue = Objects.requireNonNull(t.getConfig())
            .stream()
            .filter(conf -> retentionKey.equals(conf.getKey()))
            .findFirst();

        assertTrue(retentionValue.isPresent(), "updated config not found");
        assertEquals(retentionValue.get().getValue(), retentionTime);
    }

    @Test(dependsOnMethods = {"testUpdateTopic", "testGrantProducerAndConsumerAccess"}, enabled = true)
    @SneakyThrows
    public void testKafkaInstanceUpdatedTopic() {

        var bootstrapHost = kafka.getBootstrapServerHost();
        var clientID = serviceAccountSecret.getClientID();
        var clientSecret = serviceAccountSecret.getClientSecret();

        bwait(testTopic(
            vertx,
            bootstrapHost,
            clientID,
            clientSecret,
            TOPIC_NAME,
            1000,
            10,
            100));
    }

    @Test(dependsOnMethods = {"testGrantProducerAndConsumerAccess"}, enabled = true)
    @SneakyThrows
    public void testDescribeConsumerGroup() {

        var consumer = bwait(KafkaInstanceApiUtils.startConsumerGroup(vertx,
            CONSUMER_GROUP_NAME,
            TOPIC_NAME,
            kafka.getBootstrapServerHost(),
            serviceAccountSecret.getClientID(),
            serviceAccountSecret.getClientSecret()));
        consumer.close();

        var group = CLIUtils.waitForConsumerGroup(cli, CONSUMER_GROUP_NAME);
        LOGGER.debug(group);

        assertEquals(group.getGroupId(), CONSUMER_GROUP_NAME);
    }

    @Test(dependsOnMethods = "testDescribeConsumerGroup", enabled = true)
    @SneakyThrows
    public void testListConsumerGroups() {
        var groups = cli.listConsumerGroups();
        LOGGER.debug(groups);

        var filteredGroup = Objects.requireNonNull(groups.getItems())
            .stream()
            .filter(g -> CONSUMER_GROUP_NAME.equals(g.getGroupId()))
            .findAny();

        assertTrue(filteredGroup.isPresent());
    }

    @Test(dependsOnMethods = "testDescribeConsumerGroup", priority = 1, enabled = true)
    @SneakyThrows
    public void testDeleteConsumerGroup() {

        LOGGER.info("delete consumer group '{}'", CONSUMER_GROUP_NAME);
        cli.deleteConsumerGroup(CONSUMER_GROUP_NAME);

        assertThrows(CliNotFoundException.class,
            () -> cli.describeConsumerGroup(CONSUMER_GROUP_NAME));
    }

    @Test(dependsOnMethods = "testCreateTopic", priority = 2, enabled = true)
    @SneakyThrows
    public void testDeleteTopic() {

        LOGGER.info("delete topic '{}'", TOPIC_NAME);
        cli.deleteTopic(TOPIC_NAME);

        assertThrows(CliNotFoundException.class,
            () -> cli.describeTopic(TOPIC_NAME));
    }

    @Test(dependsOnMethods = "testCreateServiceAccount", priority = 2, enabled = true)
    @SneakyThrows
    public void testDeleteServiceAccount() {

        LOGGER.info("delete service account '{}'", serviceAccount.getClientId());
        cli.deleteServiceAccount(serviceAccount.getClientId());

        assertThrows(CliNotFoundException.class,
            () -> cli.describeServiceAccount(serviceAccount.getClientId()));
    }

    @Test(dependsOnMethods = "testCreateKafkaInstance", priority = 3, enabled = true)
    @SneakyThrows
    public void testDeleteKafkaInstance() {

        LOGGER.info("delete kafka instance '{}'", kafka.getId());
        cli.deleteKafka(kafka.getId());

        CLIUtils.waitUntilKafkaIsDeleted(cli, kafka.getId());
    }

    @Test(dependsOnMethods = "testLogin", priority = 3, enabled = true)
    @SneakyThrows
    public void testLogout() {

        LOGGER.info("verify that we are logged-in");
        cli.listKafka(); // successfully run cli command while logged in

        LOGGER.info("logout");
        cli.logout();

        LOGGER.info("verify that we are logged-in");
        assertThrows(CliGenericException.class, () -> cli.listKafka()); // unable to run the same command after logout
    }
}
