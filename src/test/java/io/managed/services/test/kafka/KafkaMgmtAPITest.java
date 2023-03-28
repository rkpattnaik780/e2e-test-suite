package io.managed.services.test.kafka;

import com.openshift.cloud.api.kas.auth.models.NewTopicInput;
import com.openshift.cloud.api.kas.auth.models.TopicSettings;
import com.openshift.cloud.api.kas.models.KafkaRequest;
import com.openshift.cloud.api.kas.models.KafkaRequestPayload;
import com.openshift.cloud.api.kas.models.KafkaUpdateRequest;
import com.openshift.cloud.api.kas.models.ServiceAccount;
import com.openshift.cloud.api.kas.models.ServiceAccountRequest;
import io.managed.services.test.Environment;
import io.managed.services.test.TestBase;
import io.managed.services.test.TestUtils;
import io.managed.services.test.ThrowingFunction;
import io.managed.services.test.client.ApplicationServicesApi;
import io.managed.services.test.client.exception.ApiConflictException;
import io.managed.services.test.client.exception.ApiGenericException;
import io.managed.services.test.client.kafka.KafkaAdminUtils;
import io.managed.services.test.client.kafka.KafkaAuthMethod;
import io.managed.services.test.client.kafka.KafkaProducerClient;
import io.managed.services.test.client.kafkainstance.KafkaInstanceApi;
import io.managed.services.test.client.kafkainstance.KafkaInstanceApiAccessUtils;
import io.managed.services.test.client.kafkainstance.KafkaInstanceApiUtils;
import io.managed.services.test.client.kafkamgmt.KafkaMgmtApi;
import io.managed.services.test.client.kafkamgmt.KafkaMgmtApiUtils;
import io.managed.services.test.client.kafkamgmt.KafkaMgmtMetricsUtils;
import io.managed.services.test.client.securitymgmt.SecurityMgmtAPIUtils;
import io.managed.services.test.client.securitymgmt.SecurityMgmtApi;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.javatuples.Pair;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Ignore;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static io.managed.services.test.TestUtils.assumeTeardown;
import static io.managed.services.test.TestUtils.bwait;
import static io.managed.services.test.TestUtils.waitFor;
import static io.managed.services.test.client.kafka.KafkaMessagingUtils.testTopic;
import static java.time.Duration.ofMinutes;
import static java.time.Duration.ofSeconds;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


/**
 * Test Kafka Mgmt API
 *
 * <p>
 * <b>Requires:</b>
 * <ul>
 *     <li> PRIMARY_OFFLINE_TOKEN
 * </ul>
 */
@Log4j2
public class KafkaMgmtAPITest extends TestBase {

    static final String SERVICE_ACCOUNT_NAME_FOR_DELETION = "mk-e2e-sa-delete"  + Environment.LAUNCH_SUFFIX;

    // TODO enterprise: change logic of name assignment back once we have quota for more than 1 enterprise kafka instance
    static final String KAFKA_INSTANCE_NAME = Environment.IS_ENTERPRISE ? "enterprise-test" : "mk-e2e-" + Environment.LAUNCH_SUFFIX;

    static final String SERVICE_ACCOUNT_NAME = "mk-e2e-sa-"  + Environment.LAUNCH_SUFFIX;
    static final String TOPIC_NAME = "test-topic";
    static final String METRIC_TOPIC_NAME = "metric-test-topic";
    static final String[] KAFKA_METRICS = {
        "kafka_server_brokertopicmetrics_messages_in_total",
        "kafka_server_brokertopicmetrics_bytes_in_total",
        "kafka_server_brokertopicmetrics_bytes_out_total",
        "kubelet_volume_stats_available_bytes",
        "kubelet_volume_stats_used_bytes",
        "kafka_broker_quota_softlimitbytes",
        "kafka_broker_quota_totalstorageusedbytes",
        "kafka_controller_kafkacontroller_offline_partitions_count",
        "kafka_controller_kafkacontroller_global_partition_count",
        "kafka_topic:kafka_log_log_size:sum",
        "kafka_namespace:haproxy_server_bytes_in_total:rate5m",
        "kafka_namespace:haproxy_server_bytes_out_total:rate5m",
        "kafka_topic:kafka_topic_partitions:sum",
        "kafka_topic:kafka_topic_partitions:count",
        "consumergroup:kafka_consumergroup_members:count",
        "kafka_namespace:kafka_server_socket_server_metrics_connection_count:sum",
        "kafka_namespace:kafka_server_socket_server_metrics_connection_creation_rate:sum",
        "kafka_topic:kafka_server_brokertopicmetrics_messages_in_total:rate5m",
        "kafka_topic:kafka_server_brokertopicmetrics_bytes_in_total:rate5m",
        "kafka_topic:kafka_server_brokertopicmetrics_bytes_out_total:rate5m"
    };

    private KafkaMgmtApi kafkaMgmtApi;
    private SecurityMgmtApi securityMgmtApi;
    private KafkaRequest kafka;
    private ServiceAccount serviceAccount;
    private KafkaInstanceApi kafkaInstanceApi;


    @BeforeClass(alwaysRun = true)
    public void bootstrap() {
        assertNotNull(Environment.PRIMARY_OFFLINE_TOKEN, "the PRIMARY_OFFLINE_TOKEN env is null");

        var apps = ApplicationServicesApi.applicationServicesApi(Environment.PRIMARY_OFFLINE_TOKEN);

        securityMgmtApi = apps.securityMgmt();
        kafkaMgmtApi = apps.kafkaMgmt();
    }

    @AfterClass(alwaysRun = true)
    public void teardown() {
        assumeTeardown();

        if (Environment.SKIP_KAFKA_TEARDOWN) {
            try {
                var request = new KafkaUpdateRequest();
                request.setReauthenticationEnabled(true);
                kafkaMgmtApi.updateKafka(kafka.getId(), request);
            } catch (Throwable t) {
                log.warn("resat kafka reauth error: ", t);
            }

            try {
                kafkaInstanceApi.deleteTopic(TOPIC_NAME);
            } catch (Throwable t) {
                log.warn("clean {} topic error: ", TOPIC_NAME, t);
            }

            try {
                kafkaInstanceApi.deleteTopic(METRIC_TOPIC_NAME);
            } catch (Throwable t) {
                log.warn("clean {} topic error: ", METRIC_TOPIC_NAME, t);
            }
        }

        // delete kafka instance
        try {
            KafkaMgmtApiUtils.cleanKafkaInstance(kafkaMgmtApi, KAFKA_INSTANCE_NAME);
        } catch (Throwable t) {
            log.error("clean main kafka instance error: ", t);
        }

        // delete service account
        try {
            SecurityMgmtAPIUtils.cleanServiceAccount(securityMgmtApi, SERVICE_ACCOUNT_NAME);
        } catch (Throwable t) {
            log.error("clean service account error: ", t);
        }

        try {
            SecurityMgmtAPIUtils.cleanServiceAccount(securityMgmtApi, SERVICE_ACCOUNT_NAME_FOR_DELETION);
        } catch (Throwable t) {
            log.error("clean service account error: ", t);
        }
    }

    @Test(groups = {"pr-check", "production"})
    @SneakyThrows
    public void testApplyKafkaInstance() {

        // Create Kafka Instance
        var payload = new KafkaRequestPayload();
        payload.setName(KAFKA_INSTANCE_NAME);
        payload.setCloudProvider(Environment.CLOUD_PROVIDER);
        payload.setRegion(Environment.DEFAULT_KAFKA_REGION);

        log.info("create kafka instance '{}'", payload.getName());
        kafka = KafkaMgmtApiUtils.applyKafkaInstance(kafkaMgmtApi, payload);

        kafkaInstanceApi = KafkaInstanceApiUtils.kafkaInstanceApi(kafka, Environment.PRIMARY_OFFLINE_TOKEN);
    }

    @Test(groups = {"pr-check", "production"})
    @SneakyThrows
    public void testCreateServiceAccount() {

        // Create Service Account
        log.info("create service account '{}'", SERVICE_ACCOUNT_NAME);
        var sa = new ServiceAccountRequest();
        sa.setName(SERVICE_ACCOUNT_NAME);
        sa.setDescription("E2E test service account");
        serviceAccount = securityMgmtApi.createServiceAccount(sa);
    }

    @Test(dependsOnMethods = {"testCreateServiceAccount", "testApplyKafkaInstance"}, groups = {"pr-check", "production"})
    @SneakyThrows
    public void testCreateProducerAndConsumerACLs() {

        var principal = KafkaInstanceApiAccessUtils.toPrincipal(serviceAccount.getClientId());
        log.info("create topic and group read and topic write ACLs for the principal '{}'", principal);

        // Create ACLs to consumer and produce messages
        KafkaInstanceApiAccessUtils.createProducerAndConsumerACLs(kafkaInstanceApi, principal);
    }

    @Test(dependsOnMethods = "testApplyKafkaInstance", groups = {"pr-check", "production"})
    @SneakyThrows
    public void testCreateTopics() {

        log.info("create topic '{}' on the instance '{}'", TOPIC_NAME, kafka.getName());
        var topicPayload = new NewTopicInput();
        topicPayload.setName(TOPIC_NAME);
        var settings = new TopicSettings();
        settings.setNumPartitions(1);
        topicPayload.setSettings(settings);
        kafkaInstanceApi.createTopic(topicPayload);

        log.info("create topic '{}' on the instance '{}'", METRIC_TOPIC_NAME, kafka.getName());
        var metricTopicPayload = new NewTopicInput();
        metricTopicPayload.setName(METRIC_TOPIC_NAME);
        var metricSettings = new TopicSettings();
        metricSettings.setNumPartitions(1);
        metricTopicPayload.setSettings(metricSettings);
        kafkaInstanceApi.createTopic(metricTopicPayload);
    }

    // ADMIN API can check that requesting creation of topic to have more partition than is max limit on given instance could not be satisfied in any case
    @Test(dependsOnMethods = {"testCreateServiceAccount", "testApplyKafkaInstance"})
    @SneakyThrows
    public void testForbiddenToExceedPartitionLimitOnTopicCreation() {

        var topicName = "topic-part-exceed-create";
        final int maxPartitionLimit = KafkaMgmtApiUtils.getPartitionLimitMax(kafkaMgmtApi, kafka);
        log.info("Max partition limit per given instance: {}", maxPartitionLimit);

        log.info("Attempt to create topic '{}', with too many partitions: {}", topicName, maxPartitionLimit + 1);
        var payload = new NewTopicInput();
        payload.setName(topicName);
        var settings = new TopicSettings();
        settings.setNumPartitions(maxPartitionLimit + 1);
        payload.setSettings(settings);
        assertThrows(ApiGenericException.class,
                () -> kafkaInstanceApi.createTopic(payload));
    }

    // ADMIN API can check that requesting to change configuration of a topic to have more partition than is max limit on given instance could not be satisfied in any case
    @Test(dependsOnMethods = {"testCreateServiceAccount", "testApplyKafkaInstance"})
    @SneakyThrows
    public void testForbiddenToExceedParitionLimitOnTopicConfiguration() {

        var topicName = "topic-part-exceed-conf";
        KafkaInstanceApiUtils.applyTopic(kafkaInstanceApi, topicName);

        final int maxPartitionLimit = KafkaMgmtApiUtils.getPartitionLimitMax(kafkaMgmtApi, kafka);
        assertThrows(ApiGenericException.class,
                () -> KafkaInstanceApiUtils.updateTopicPartition(kafkaInstanceApi, topicName, maxPartitionLimit + 1));
    }

    // Requesting valid number of partition in created topic, but breaching the limit by Sum of partitions with all of existing Topic (i.e. topic visible to user)
    @Test(dependsOnMethods = {"testCreateServiceAccount", "testApplyKafkaInstance"})
    @SneakyThrows
    public void testTotalPartitionLimitByTopicCreation() {

        log.info("2 topics are to be created, creation of second one is supposed to exceed max limit of partitions");
        var topicNameFirst = "topic-part-total-exceed-create";
        var topicNameSecond = "topic-part-total-exceed-2-create";
        final int maxPartitionLimit = KafkaMgmtApiUtils.getPartitionLimitMax(kafkaMgmtApi, kafka);
        final int currentPartitionCount = KafkaInstanceApiUtils.getPartitionCountTotal(kafkaInstanceApi);

        // Assuming we are not already overflowing given partition limit
        log.info("Asserting no excessive number of partitions is created");
        if (currentPartitionCount > maxPartitionLimit) {
            throw new SkipException("Already overflowing partition limit");
        }

        // create 2 topic, first can be created but spend half of free partitions, Second fails as it will breach the max limit
        final int partitionCountOfNewTopic = (maxPartitionLimit - currentPartitionCount) / 2;
        log.info("Creating first topic '{}'", topicNameFirst);
        // first topic
        var payloadTopic1 = new NewTopicInput();
        payloadTopic1.setName(topicNameFirst);
        var settings1 = new TopicSettings();
        settings1.setNumPartitions(partitionCountOfNewTopic);
        payloadTopic1.setSettings(settings1);
        // creation of topic with big number of partition may take long time, which result in repetition of API call due to timeout, but topic will be created
        try {
            kafkaInstanceApi.createTopic(payloadTopic1);
        } catch (ApiConflictException e) {
            log.warn("creation of topic {} takes long time to respond", payloadTopic1.getName());
            log.info("Sleeping for additional 20 seconds due to slow response");
            Thread.sleep(ofSeconds(20).toMillis());
        }
        // wait to make sure changes were propagated
        log.info("Sleeping 20 seconds before creating second topic '{}'", topicNameSecond);
        Thread.sleep(ofSeconds(20).toMillis());
        // second topic, which should overflow number of allowed partitions. 3 is just to make sure it really overflow given number (1 would fail in case of odd number of partitions)
        log.info("Creating second topic '{}'", topicNameSecond);
        var payloadTopic2 = new NewTopicInput();
        payloadTopic2.setName(topicNameSecond);
        var settings2 = new TopicSettings();
        settings2.setNumPartitions(partitionCountOfNewTopic + 3);
        payloadTopic2.setSettings(settings2);
        assertThrows(ApiGenericException.class, () -> kafkaInstanceApi.createTopic(payloadTopic2));

        // cleanup
        try {
            kafkaInstanceApi.deleteTopic(topicNameFirst);
        } catch (Throwable t) {
            log.warn("clean {} topic error: ", topicNameFirst, t);
        }
        try {
            kafkaInstanceApi.deleteTopic(topicNameSecond);
        } catch (Throwable t) {
            log.warn("clean {} topic error: ", topicNameSecond, t);
        }

        // there is a need to wait for some period of time before continue with other test as they still may need to create topics (i.e. extra partitions)
        log.info("wait 15 seconds before making sure changes in partition count were propagated");
        Thread.sleep(ofSeconds(20).toMillis());
    }

    @Test(dependsOnMethods = {"testCreateServiceAccount", "testApplyKafkaInstance"})
    @SneakyThrows
    public void testTotalPartitionLimitByTopicConfiguration() {

        final var topicFillPartitionCountName = "topic-fill-partition-count";
        final var topicName = "topic-part-total-exceed-conf";

        log.info("Apply topic with 1 partition");
        KafkaInstanceApiUtils.applyTopic(kafkaInstanceApi, topicName);

        log.info("Create topic with partitions so that there is still 1 more partition within limit");
        final int maxPartitionLimit = KafkaMgmtApiUtils.getPartitionLimitMax(kafkaMgmtApi, kafka);
        final int currentPartitionCount = KafkaInstanceApiUtils.getPartitionCountTotal(kafkaInstanceApi);
        var payloadTopicPartitionFiller = new NewTopicInput();
        payloadTopicPartitionFiller.setName(topicFillPartitionCountName);
        var settings = new TopicSettings();
        settings.setNumPartitions(maxPartitionLimit - currentPartitionCount - 1);
        payloadTopicPartitionFiller.setSettings(settings);

        // creation of topic with big number of partition may take long time, which result in repetition of API call due to timeout, but topic will be created
        try {
            kafkaInstanceApi.createTopic(payloadTopicPartitionFiller);
        } catch (ApiConflictException e) {
            log.warn("creation of topic {} takes long time to respond", payloadTopicPartitionFiller.getName());
            log.info("Sleeping for additional 20 seconds due to slow response");
            Thread.sleep(ofSeconds(20).toMillis());
        }

        // there is a need to wait for some period of time before actually,
        log.info("wait 15 seconds before making sure changes in partition count were propagated");
        Thread.sleep(ofSeconds(15).toMillis());

        log.info("Increase partition count on topic to reach the total limit");
        KafkaInstanceApiUtils.updateTopicPartition(kafkaInstanceApi, topicName, 2);

        // there is a need to wait for some period of time before actually,
        log.info("wait 15 seconds before making sure changes in partition count were propagated");
        Thread.sleep(ofSeconds(15).toMillis());

        log.info("Increase partition count on topic to exceed the total partition limit");
        assertThrows(ApiGenericException.class, () -> KafkaInstanceApiUtils.updateTopicPartition(kafkaInstanceApi, topicName, 3));

        // cleanup
        try {
            kafkaInstanceApi.deleteTopic(topicName);
        } catch (Throwable t) {
            log.warn("clean {} topic error: ", topicName, t);
        }
        try {
            kafkaInstanceApi.deleteTopic(topicFillPartitionCountName);
        } catch (Throwable t) {
            log.warn("clean {} topic error: ", topicFillPartitionCountName, t);
        }

        // there is a need to wait for some period of time before continue with other test as they still may need to create topics (i.e. extra partitions)
        log.info("wait 15 seconds before making sure changes in partition count were propagated");
        Thread.sleep(ofSeconds(15).toMillis());
    }

    @Test(dependsOnMethods = {"testCreateTopics", "testCreateProducerAndConsumerACLs"})
    @SneakyThrows
    public void testMessageInTotalMetric() {

        log.info("test message in total metric");
        KafkaMgmtMetricsUtils.testMessageInTotalMetric(kafkaMgmtApi, kafka, serviceAccount, TOPIC_NAME);
    }

    @Test(dependsOnMethods = {"testCreateTopics", "testCreateProducerAndConsumerACLs"}, groups = {"pr-check", "production"})
    @SneakyThrows
    public void testMessagingKafkaInstanceUsingOAuth() {

        var bootstrapHost = kafka.getBootstrapServerHost();
        var clientID = serviceAccount.getClientId();
        var clientSecret = serviceAccount.getClientSecret();

        bwait(testTopic(
            Vertx.vertx(),
            bootstrapHost,
            clientID,
            clientSecret,
            TOPIC_NAME,
            1000,
            10,
            100,
            KafkaAuthMethod.OAUTH));
    }

    @Test(dependsOnMethods = {"testCreateTopics", "testCreateProducerAndConsumerACLs"})
    @SneakyThrows
    public void testFailedToMessageKafkaInstanceUsingOAuthAndFakeSecret() {

        var bootstrapHost = kafka.getBootstrapServerHost();
        var clientID = serviceAccount.getClientId();

        assertThrows(KafkaException.class, () -> bwait(testTopic(
            Vertx.vertx(),
            bootstrapHost,
            clientID,
            "invalid",
            TOPIC_NAME,
            1,
            10,
            11,
            KafkaAuthMethod.OAUTH)));
    }

    @Test(dependsOnMethods = {"testCreateTopics", "testCreateProducerAndConsumerACLs"}, groups = {"pr-check", "production"})
    @SneakyThrows
    public void testMessagingKafkaInstanceUsingPlainAuth() {

        var bootstrapHost = kafka.getBootstrapServerHost();
        var clientID = serviceAccount.getClientId();
        var clientSecret = serviceAccount.getClientSecret();

        bwait(testTopic(
            Vertx.vertx(),
            bootstrapHost,
            clientID,
            clientSecret,
            TOPIC_NAME,
            1000,
            10,
            100,
            KafkaAuthMethod.PLAIN));
    }

    @Test(dependsOnMethods = {"testCreateTopics", "testCreateProducerAndConsumerACLs"})
    @SneakyThrows
    public void testFailedToMessageKafkaInstanceUsingPlainAuthAndFakeSecret() {

        var bootstrapHost = kafka.getBootstrapServerHost();
        var clientID = serviceAccount.getClientId();

        assertThrows(KafkaException.class, () -> bwait(testTopic(
            Vertx.vertx(),
            bootstrapHost,
            clientID,
            "invalid",
            TOPIC_NAME,
            1,
            10,
            11,
            KafkaAuthMethod.PLAIN)));
    }

    @Test(dependsOnMethods = {"testApplyKafkaInstance"})
    @SneakyThrows
    public <T extends Throwable> void testFederateMetrics() {
        // Verify all expected user facing Kafka metrics retrieved from Observatorium are included in the response in a Prometheus Text Format
        var missingMetricsAtom = new AtomicReference<List<String>>();
        ThrowingFunction<Boolean, Boolean, ApiGenericException> isMetricAvailable = last -> {
            var metrics = kafkaMgmtApi.federateMetrics(kafka.getId());
            log.debug(metrics);

            var missingMetrics = new ArrayList<String>();
            for (var metricName : KAFKA_METRICS) {
                var metricTypeDefinition = String.format("# TYPE %s gauge", metricName);
                if (!metrics.contains(metricTypeDefinition)) missingMetrics.add(metricName);
            }

            missingMetricsAtom.set(missingMetrics);
            log.debug("missing metrics: {}", missingMetrics);

            return missingMetrics.size() == 0;
        };
        try {
            waitFor("all federated metrics to become available", ofSeconds(3), ofMinutes(5), isMetricAvailable);
        } catch (TimeoutException e) {
            throw new AssertionError(TestUtils.message("Missing metrics: expected metrics: {} but missing: {}", KAFKA_METRICS, missingMetricsAtom.get()), e);
        }
    }

    @Test(dependsOnMethods = {"testApplyKafkaInstance"}, groups = {"pr-check", "production"})
    @SneakyThrows
    public void testListAndSearchKafkaInstance() {

        // TODO: Split in between list kafka and search kafka by name

        // List kafka instances
        log.info("get kafka list");
        var kafkaList = kafkaMgmtApi.getKafkas(null, null, null, null);

        log.debug(kafkaList);
        assertTrue(kafkaList.getItems().size() > 0);

        // Get created kafka instance from the list
        log.info("find kafka instance '{}' in list", KAFKA_INSTANCE_NAME);
        var findKafka = kafkaList.getItems().stream()
            .filter(k -> KAFKA_INSTANCE_NAME.equals(k.getName()))
            .findAny();
        log.debug(findKafka.orElse(null));
        assertTrue(findKafka.isPresent());

        // Search kafka by name
        log.info("search kafka instance '{}' by name", KAFKA_INSTANCE_NAME);
        var kafkaOptional = KafkaMgmtApiUtils.getKafkaByName(kafkaMgmtApi, KAFKA_INSTANCE_NAME);
        log.debug(kafkaOptional.orElse(null));
        assertTrue(kafkaOptional.isPresent());
        assertEquals(kafkaOptional.get().getName(), KAFKA_INSTANCE_NAME);
    }

    @Test(dependsOnMethods = {"testApplyKafkaInstance"})
    public void testFailToCreateKafkaInstanceIfItAlreadyExist() {

        // Create Kafka Instance with existing name
        var payload = new KafkaRequestPayload();
        payload.setName(KAFKA_INSTANCE_NAME);
        payload.setCloudProvider(Environment.CLOUD_PROVIDER);
        payload.setRegion(Environment.DEFAULT_KAFKA_REGION);

        log.info("create kafka instance '{}' with existing name", payload.getName());
        assertThrows(ApiConflictException.class, () -> kafkaMgmtApi.createKafka(true, payload));
    }

    // TODO change implementation so that there is no endless trying to reauth, (8 times/sec for rest of rests)
    @Ignore
    @Test(dependsOnMethods = {"testCreateServiceAccount", "testApplyKafkaInstance"}, priority = 1)
    @SneakyThrows
    public void testReauthentication() {

        // make sure reauthentication is enabled by default
        assertTrue(kafka.getReauthenticationEnabled());

        var initialSessionLifetimeMs = KafkaAdminUtils.getAuthenticatorPositiveSessionLifetimeMs(
            kafka.getBootstrapServerHost(),
            serviceAccount.getClientId(),
            serviceAccount.getClientSecret());
        log.debug("positiveSessionLifetimeMs: {}", initialSessionLifetimeMs);
        // because reauth is enabled the session lifetime can not be null
        assertNotNull(initialSessionLifetimeMs);

        // disable kafka instance reauthentication
        log.info("set Kafka reauthentication to false");
        var request = new KafkaUpdateRequest();
        request.setReauthenticationEnabled(false);
        kafka = kafkaMgmtApi.updateKafka(kafka.getId(), request);
        assertFalse(kafka.getReauthenticationEnabled());

        ThrowingFunction<Boolean, Boolean, ApiGenericException> isReauthenticationDisabled = last -> {

            var sessionLifetimeMs = KafkaAdminUtils.getAuthenticatorPositiveSessionLifetimeMs(
                kafka.getBootstrapServerHost(),
                serviceAccount.getClientId(),
                serviceAccount.getClientSecret());

            log.debug("positiveSessionLifetimeMs: {}", sessionLifetimeMs);

            // session lifetime should become null after disabling reauth
            return sessionLifetimeMs == null;
        };
        waitFor("kafka reauthentication to be disabled", ofSeconds(10), ofMinutes(5), isReauthenticationDisabled);
    }

    @Test(dependsOnMethods = "testCreateTopics", groups = {"pr-check", "production"})
    @SneakyThrows
    public void testDeleteServiceAccount() {

        // create SA specifically for purpose of demonstration that it works, afterwards deleting it and fail to use it anymore
        log.info("create service account '{}'", SERVICE_ACCOUNT_NAME_FOR_DELETION);
        var sa = new ServiceAccountRequest();
        sa.setName(SERVICE_ACCOUNT_NAME_FOR_DELETION);
        sa.setDescription("E2E test service account");
        var serviceAccountForDeletion = securityMgmtApi.createServiceAccount(sa);

        // ACLs
        var principal = KafkaInstanceApiAccessUtils.toPrincipal(serviceAccountForDeletion.getClientId());
        log.info("create topic and group read and topic write ACLs for the principal '{}'", principal);
        KafkaInstanceApiAccessUtils.createProducerAndConsumerACLs(kafkaInstanceApi, principal);

        // working Communication (Producing & Consuming) using  SA (serviceAccountForDeletion)
        var bootstrapHost = kafka.getBootstrapServerHost();
        var clientID = serviceAccountForDeletion.getClientId();
        var clientSecret = serviceAccountForDeletion.getClientSecret();

        bwait(testTopic(
            Vertx.vertx(),
            bootstrapHost,
            clientID,
            clientSecret,
            TOPIC_NAME,
            1000,
            10,
            100,
            KafkaAuthMethod.PLAIN));

        // deletion of SA (serviceAccountForDeletion)
        securityMgmtApi.deleteServiceAccountById(serviceAccountForDeletion.getId());

        // fail to communicate due to service account being deleted using PLAIN & OAUTH
        assertThrows(KafkaException.class, () -> {
            bwait(testTopic(
                Vertx.vertx(),
                bootstrapHost,
                clientID,
                clientSecret,
                TOPIC_NAME,
                1000,
                10,
                100,
                KafkaAuthMethod.PLAIN
            ));
        });
        assertThrows(KafkaException.class, () -> {
            bwait(testTopic(
                Vertx.vertx(),
                bootstrapHost,
                clientID,
                clientSecret,
                TOPIC_NAME,
                1000,
                10,
                100,
                KafkaAuthMethod.OAUTH
            ));
        });
    }

    @Test(dependsOnMethods = {"testApplyKafkaInstance"}, groups = {"pr-check", "production"}, priority = 2)
    @SneakyThrows
    public void testDeleteKafkaInstance() {
        if (Environment.SKIP_KAFKA_TEARDOWN || Environment.IS_ENTERPRISE) {
            throw new SkipException("Skip kafka delete");
        }

        var bootstrapHost = kafka.getBootstrapServerHost();
        var clientID = serviceAccount.getClientId();
        var clientSecret = serviceAccount.getClientSecret();

        // Connect the Kafka producer
        log.info("initialize kafka producer");
        var producer = new KafkaProducerClient<>(
            Vertx.vertx(),
            bootstrapHost,
            clientID,
            clientSecret,
            KafkaAuthMethod.PLAIN,
            StringSerializer.class,
            StringSerializer.class,
            new HashMap<>());

        // Delete the Kafka instance
        log.info("delete kafka instance '{}'", KAFKA_INSTANCE_NAME);
        kafkaMgmtApi.deleteKafkaById(kafka.getId(), true);
        KafkaMgmtApiUtils.waitUntilKafkaIsDeleted(kafkaMgmtApi, kafka.getId());

        // Produce Kafka messages
        log.info("send message to topic '{}'", TOPIC_NAME);
        waitFor(Vertx.vertx(), "sent message to fail", ofSeconds(3), ofSeconds(30), last ->
            producer.send(KafkaProducerRecord.create(TOPIC_NAME, "hello world"))
                .compose(
                    __ -> Future.succeededFuture(Pair.with(false, null)),
                    t -> Future.succeededFuture(Pair.with(true, null))));

        log.info("close kafka producer and consumer");
        bwait(producer.asyncClose());
    }

}
