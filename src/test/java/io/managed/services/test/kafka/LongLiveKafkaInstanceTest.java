package io.managed.services.test.kafka;


import com.openshift.cloud.api.kas.auth.models.NewTopicInput;
import com.openshift.cloud.api.kas.auth.models.TopicSettings;
import com.openshift.cloud.api.kas.models.KafkaRequest;
import com.openshift.cloud.api.kas.models.ServiceAccount;
import io.managed.services.test.Environment;
import io.managed.services.test.TestBase;
import io.managed.services.test.client.ApplicationServicesApi;
import io.managed.services.test.client.exception.ApiGenericException;
import io.managed.services.test.client.kafka.KafkaAdmin;
import io.managed.services.test.client.kafka.KafkaAuthMethod;
import io.managed.services.test.client.kafka.KafkaConsumerClient;
import io.managed.services.test.client.kafkainstance.KafkaInstanceApi;
import io.managed.services.test.client.kafkainstance.KafkaInstanceApiAccessUtils;
import io.managed.services.test.client.kafkainstance.KafkaInstanceApiUtils;
import io.managed.services.test.client.kafkamgmt.KafkaMgmtApi;
import io.managed.services.test.client.kafkamgmt.KafkaMgmtApiUtils;
import io.managed.services.test.client.kafkamgmt.KafkaMgmtMetricsUtils;
import io.managed.services.test.client.securitymgmt.SecurityMgmtAPIUtils;
import io.managed.services.test.client.securitymgmt.SecurityMgmtApi;
import io.vertx.core.Vertx;
import lombok.SneakyThrows;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static io.managed.services.test.TestUtils.bwait;
import static io.managed.services.test.TestUtils.message;
import static io.managed.services.test.client.kafka.KafkaMessagingUtils.testTopic;
import static io.managed.services.test.client.kafka.KafkaMessagingUtils.testTopicWithMultipleConsumers;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;


/**
 * Test a Long Live Kafka Instance that will be deleted only before it expires, but otherwise it will continue to run
 * also after the tests are concluded. This is a very basic test to ensure that an update doesn't break
 * an existing Kafka Instance.
 * <p>
 * <b>Requires:</b>
 * <ul>
 *     <li> PRIMARY_USERNAME
 *     <li> PRIMARY_PASSWORD
 * </ul>
 */
public class LongLiveKafkaInstanceTest extends TestBase {
    private static final Logger LOGGER = LogManager.getLogger(LongLiveKafkaInstanceTest.class);

    public static final String KAFKA_INSTANCE_NAME = "mk-e2e-ll-" + Environment.LAUNCH_KEY;
    public static final String SERVICE_ACCOUNT_NAME = "mk-e2e-ll-sa-" + Environment.LAUNCH_KEY;

    private static final String TOPIC_NAME = "test-topic";
    private static final String MULTI_PARTITION_TOPIC_NAME = "multi-partitions-topic";
    private static final String METRIC_TOPIC_NAME = "metric-test-topic";

    static final String TEST_CANARY_NAME = "__strimzi_canary";
    public static final String TEST_CANARY_GROUP = "canary-group";

    private KafkaMgmtApi kafkaMgmtApi;
    private SecurityMgmtApi securityMgmtApi;
    private KafkaInstanceApi kafkaInstanceApi;

    private KafkaRequest kafka;
    private ServiceAccount serviceAccount;

    @BeforeClass
    public void bootstrap() {
        assertNotNull(Environment.PRIMARY_OFFLINE_TOKEN, "the PRIMARY_OFFLINE_TOKEN env is null");

        var apps = ApplicationServicesApi.applicationServicesApi(Environment.PRIMARY_OFFLINE_TOKEN);

        this.kafkaMgmtApi = apps.kafkaMgmt();
        this.securityMgmtApi = apps.securityMgmt();
    }

    @Test
    @SneakyThrows
    public void testThatTheLongLiveKafkaInstanceAlreadyExist() {

        LOGGER.info("get kafka instance '{}'", KAFKA_INSTANCE_NAME);
        var optionalKafka = KafkaMgmtApiUtils.getKafkaByName(kafkaMgmtApi, KAFKA_INSTANCE_NAME);
        if (optionalKafka.isEmpty()) {
            fail(message("the long living kafka instance '{}' doesn't exist", KAFKA_INSTANCE_NAME));
        }
        kafka = optionalKafka.get();
        LOGGER.debug(kafka);
    }

    @Test(priority = 1)
    @SneakyThrows
    public void testRecreateTheLongLiveKafkaInstanceIfItDoesNotExist() {
        // recreate the instance only if it doesn't exist
        if (Objects.isNull(kafka)) {
            LOGGER.info("create kafka instance '{}'", KAFKA_INSTANCE_NAME);
            kafka = KafkaMgmtApiUtils.applyKafkaInstance(kafkaMgmtApi, KAFKA_INSTANCE_NAME);
        }

        LOGGER.info("initialize kafka instance api");
        kafkaInstanceApi = KafkaInstanceApiUtils.kafkaInstanceApi(kafka, Environment.PRIMARY_OFFLINE_TOKEN);
    }

    @Test(dependsOnMethods = "testRecreateTheLongLiveKafkaInstanceIfItDoesNotExist")
    @SneakyThrows
    public void testThatTheLongLiveServiceAccountAlreadyExist() {

        LOGGER.info("get service account '{}'", SERVICE_ACCOUNT_NAME);
        var optionalAccount = SecurityMgmtAPIUtils.getServiceAccountByName(securityMgmtApi, SERVICE_ACCOUNT_NAME);
        if (optionalAccount.isEmpty()) {
            fail(message("the long living service account with name '{}' doesn't exist", SERVICE_ACCOUNT_NAME));
        }

        LOGGER.info("reset credentials for service account '{}'", SERVICE_ACCOUNT_NAME);
        serviceAccount = securityMgmtApi.resetServiceAccountCreds(optionalAccount.get().getId());
        createACLs(serviceAccount);
    }

    @Test(priority = 1, dependsOnMethods = "testRecreateTheLongLiveKafkaInstanceIfItDoesNotExist")
    @SneakyThrows
    public void testRecreateTheLongLiveServiceAccountIfItDoesNotExist() {
        // recreate the account only if it doesn't exist, in this case we also create ACL accordingly.
        if (Objects.isNull(serviceAccount)) {
            LOGGER.info("create service account '{}'", SERVICE_ACCOUNT_NAME);
            serviceAccount = SecurityMgmtAPIUtils.applyServiceAccount(securityMgmtApi, SERVICE_ACCOUNT_NAME);
            createACLs(serviceAccount);
        }
    }

    private void createACLs(ServiceAccount serviceAccount) throws ApiGenericException {
        if (serviceAccount != null) {
            var principal = KafkaInstanceApiAccessUtils.toPrincipal(serviceAccount.getClientId());
            KafkaInstanceApiAccessUtils.createProducerAndConsumerACLs(kafkaInstanceApi, principal);
        }
    }

    private Map<String, NewTopicInput> expectedTopics() {
        BiFunction<String, Integer, NewTopicInput> topic = (String topicName, Integer numPartitions) ->
            new NewTopicInput()
                .name(topicName)
                .settings(new TopicSettings().numPartitions(numPartitions));

        var map = new HashMap<String, NewTopicInput>();
        map.put(TOPIC_NAME, topic.apply(TOPIC_NAME, 1));
        map.put(METRIC_TOPIC_NAME, topic.apply(METRIC_TOPIC_NAME, 3));
        map.put(MULTI_PARTITION_TOPIC_NAME, topic.apply(MULTI_PARTITION_TOPIC_NAME, 3));
        return map;
    }

    @Test(dependsOnMethods = "testRecreateTheLongLiveKafkaInstanceIfItDoesNotExist")
    public void testThatAllLongLiveTopicsAlreadyExist() throws Throwable {

        final var expectedTopics = expectedTopics();

        final var topicList = kafkaInstanceApi.getTopics();
        Objects.requireNonNull(topicList.getItems()).forEach(t -> expectedTopics.remove(t.getName()));

        assertTrue(expectedTopics.isEmpty(), message("the topics '{}' don't exist", expectedTopics.keySet()));
    }

    @Test(priority = 2, dependsOnMethods = "testRecreateTheLongLiveKafkaInstanceIfItDoesNotExist")
    @SneakyThrows
    public void testRecreateLongLiveTopicsIfTheyDoNotExist() {

        final var expectedTopics = expectedTopics();

        final var topicList = kafkaInstanceApi.getTopics();
        final var unexpectedTopics = Objects.requireNonNull(topicList.getItems())
            .stream()
            .filter(t -> Objects.isNull(expectedTopics.remove(t.getName())))
            .collect(Collectors.toList());

        for (var topic : unexpectedTopics) {
            LOGGER.info("delete unexpected topic '{}'", topic.getName());
            kafkaInstanceApi.deleteTopic(topic.getName());
        }

        for (var topic : expectedTopics.values()) {
            LOGGER.info("create expected topic '{}'", topic.getName());
            kafkaInstanceApi.createTopic(topic);
        }
    }


    @Test(dependsOnMethods = {
        "testRecreateTheLongLiveKafkaInstanceIfItDoesNotExist",
        "testRecreateTheLongLiveServiceAccountIfItDoesNotExist"
    }, enabled = false)
    void testThatTheCanaryTopicExist() {

        var bootstrapHost = kafka.getBootstrapServerHost();
        var clientID = serviceAccount.getClientId();
        var clientSecret = serviceAccount.getClientSecret();

        try (var admin = new KafkaAdmin(bootstrapHost, clientID, clientSecret)) {
            var topics = admin.listTopics();
            LOGGER.debug(topics);

            var topicOptional = topics.stream().filter(e -> e.equals(TEST_CANARY_NAME)).findAny();
            assertTrue(topicOptional.isPresent());
        }
    }


    @Test(dependsOnMethods = "testThatTheCanaryTopicExist", enabled = false)
    void testCanaryLiveliness() throws Throwable {

        var bootstrapHost = kafka.getBootstrapServerHost();
        var clientID = serviceAccount.getClientId();
        var secret = serviceAccount.getClientSecret();

        try (var consumerClient = new KafkaConsumerClient<>(
            Vertx.vertx(),
            bootstrapHost,
            clientID, secret,
            KafkaAuthMethod.OAUTH,
            TEST_CANARY_GROUP,
            "latest",
            StringDeserializer.class,
            StringDeserializer.class)) {

            var asyncRecords = bwait(consumerClient.receiveAsync(TEST_CANARY_NAME, 1));

            LOGGER.info("wait for records");
            var records = bwait(asyncRecords);
            LOGGER.debug(records);
        }
    }


    @Test(dependsOnMethods = {
        "testRecreateTheLongLiveKafkaInstanceIfItDoesNotExist",
        "testRecreateLongLiveTopicsIfTheyDoNotExist",
        "testRecreateTheLongLiveServiceAccountIfItDoesNotExist"
    })
    public void testProduceAndConsumeKafkaMessages() throws Throwable {

        String bootstrapHost = kafka.getBootstrapServerHost();
        String clientID = serviceAccount.getClientId();
        String clientSecret = serviceAccount.getClientSecret();

        LOGGER.info("test topic '{}'", TOPIC_NAME);
        bwait(testTopic(Vertx.vertx(),
            bootstrapHost,
            clientID,
            clientSecret,
                TOPIC_NAME,
            10,
            7,
            10));
    }

    @Test(dependsOnMethods = {
        "testRecreateTheLongLiveKafkaInstanceIfItDoesNotExist",
        "testRecreateLongLiveTopicsIfTheyDoNotExist",
        "testRecreateTheLongLiveServiceAccountIfItDoesNotExist"
    })
    void testTopicWithThreePartitionsAndThreeConsumers() throws Throwable {

        var bootstrapHost = kafka.getBootstrapServerHost();
        var clientID = serviceAccount.getClientId();
        var clientSecret = serviceAccount.getClientSecret();

        LOGGER.info("test topic '{}' with 3 consumers", MULTI_PARTITION_TOPIC_NAME);
        bwait(testTopicWithMultipleConsumers(Vertx.vertx(),
            bootstrapHost,
            clientID,
            clientSecret,
            MULTI_PARTITION_TOPIC_NAME,
            Duration.ofMinutes(1),
            1024 * 2,
            1024,
            1024 * 4,
            3));
    }

    @Test(dependsOnMethods = {
        "testRecreateTheLongLiveKafkaInstanceIfItDoesNotExist",
        "testRecreateLongLiveTopicsIfTheyDoNotExist",
        "testRecreateTheLongLiveServiceAccountIfItDoesNotExist"
    })
    public void testMessageInTotalMetric() throws Throwable {

        LOGGER.info("test message in total metric");
        KafkaMgmtMetricsUtils.testMessageInTotalMetric(kafkaMgmtApi, kafka, serviceAccount, METRIC_TOPIC_NAME);
    }
}

