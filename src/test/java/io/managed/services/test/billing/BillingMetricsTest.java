package io.managed.services.test.billing;

import com.openshift.cloud.api.kas.auth.models.NewTopicInput;
import com.openshift.cloud.api.kas.auth.models.Topic;
import com.openshift.cloud.api.kas.auth.models.TopicSettings;
import com.openshift.cloud.api.kas.models.KafkaRequest;
import com.openshift.cloud.api.kas.models.ServiceAccount;
import io.managed.services.test.Environment;
import io.managed.services.test.TestBase;
import io.managed.services.test.client.ApplicationServicesApi;
import io.managed.services.test.client.exception.ApiGenericException;
import io.managed.services.test.client.kafka.KafkaAuthMethod;
import io.managed.services.test.client.kafka.KafkaMessagingUtils;
import io.managed.services.test.client.kafka.KafkaProducerClient;
import io.managed.services.test.client.kafkainstance.KafkaInstanceApi;
import io.managed.services.test.client.kafkainstance.KafkaInstanceApiAccessUtils;
import io.managed.services.test.client.kafkainstance.KafkaInstanceApiUtils;
import io.managed.services.test.client.kafkamgmt.KafkaMgmtApi;
import io.managed.services.test.client.kafkamgmt.KafkaMgmtApiUtils;
import io.managed.services.test.client.kafkamgmt.KafkaMgmtMetricsUtils;
import io.managed.services.test.client.securitymgmt.SecurityMgmtAPIUtils;
import io.managed.services.test.client.securitymgmt.SecurityMgmtApi;
import io.managed.services.test.prometheuswebclient.PrometheusWebClient;
import io.managed.services.test.prometheuswebclient.PrometheusWebClientBuilder;
import io.managed.services.test.prometheuswebclient.QueryResult;
import io.vertx.core.Vertx;
import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.common.serialization.StringSerializer;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.managed.services.test.TestUtils.bwait;
import static io.managed.services.test.TestUtils.message;
import static io.managed.services.test.client.kafka.KafkaMessagingUtils.testTopicWithNConsumers;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.fail;


/**
 * Test depends on Long Live Kafka Instance, if instance isn't existing at time of test start test create new one but fails.
 * Test also needs Observatorium container/ process to be running (hack/run-token-refresher.sh),
 * and also access to prometheus (hack/obtain-prom-query-token.sh) which will provide expected PROMETHEUS_WEB_CLIENT_ACCESS_TOKEN
 * <p>
 * <b>Requires:</b>
 * <ul>
 *     <li> PRIMARY_USERNAME
 *     <li> PRIMARY_PASSWORD
 *     <li> PROMETHEUS_WEB_CLIENT_ACCESS_TOKEN
 * </ul>
 */
@Log4j2
public class BillingMetricsTest extends TestBase {

    // class uses long live kafka instance, as waiting for metrics to be available and stable in newly created instance can take too long.
    public static final String KAFKA_INSTANCE_NAME = "mk-e2e-ll-" + Environment.LAUNCH_KEY;
    public static final String SERVICE_ACCOUNT_NAME = "mk-billing-sa-" + Environment.LAUNCH_KEY;

    public static final String TOPIC_NAME = "billing_test";

    private PrometheusWebClient observatoriumClient;
    private PrometheusWebClient prometheusWebClient;

    private KafkaRequest kafka;
    private ServiceAccount serviceAccount;

    private KafkaInstanceApi kafkaInstanceApi;

    private Map<String, PrometheusWebClient.Snapshot> metricToSnapshotMap = new HashMap<>();

    // TODO metrics will soon be renamed to match new naming convention, more at MGDSTRM-7080.
    private static final String METRIC_STORAGE = "kafka_broker_quota_totalstorageusedbytes";
    private static final String METRIC_TRAFFIC_IN = "kafka_instance_incoming_byte_total";
    private static final String METRIC_TRAFFIC_OUT = "kafka_instance_outgoing_byte_total";
    private static final String METRIC_CLUSTER_HOURS = "kafka_id:strimzi_resource_state:max_over_time1h";

    // size and count of messages to be produced/ consumed
    private final int messageSize = 1024 * 128;
    private final int messageCount = 40;
    // number of consumers consuming data
    private final int consumerCount = 3;

    @BeforeClass
    @SneakyThrows
    public void bootstrap() {
        assertNotNull(Environment.PRIMARY_OFFLINE_TOKEN, "the PRIMARY_OFFLINE_TOKEN env is null");
        assertNotNull(Environment.PROMETHEUS_WEB_CLIENT_ACCESS_TOKEN, "the PROMETHEUS_WEB_CLIENT_ACCESS_TOKEN env is null");

        var apps = ApplicationServicesApi.applicationServicesApi(Environment.PRIMARY_OFFLINE_TOKEN);

        KafkaMgmtApi kafkaMgmtApi = apps.kafkaMgmt();
        SecurityMgmtApi securityMgmtApi = apps.securityMgmt();

        // clients
        // build observatorium client, this client points on locally running container/process
        this.observatoriumClient = new PrometheusWebClientBuilder()
            .withBaseUrl("http://localhost:8085")
            .withUrlResourcePath("/api/metrics/v1/managedkafka/api/v1/query")
            .build();
        // build prometheus client with all necessary config, this clients points on server running on oc cluster
        this.prometheusWebClient = new PrometheusWebClientBuilder()
            .withBaseUrl(Environment.PROMETHEUS_WEB_CLIENT_ROUTE)
            .withUrlResourcePath("/api/v1/query")
            .addHeaderEntry("Authorization", "Bearer " + Environment.PROMETHEUS_WEB_CLIENT_ACCESS_TOKEN)
            .build();

        // Create Kafka Instance
        var optionalKafkaRequest = KafkaMgmtApiUtils.getKafkaByName(kafkaMgmtApi, KAFKA_INSTANCE_NAME);
        if (optionalKafkaRequest.isPresent() && optionalKafkaRequest.get().getStatus().equals("ready")) {
            kafka = optionalKafkaRequest.get();
        } else {
            KafkaMgmtApiUtils.createKafkaInstance(kafkaMgmtApi, KafkaMgmtApiUtils.defaultKafkaInstance(KAFKA_INSTANCE_NAME));
            fail(message("the billing metric can be applied only on existing kafka instance, '{}' did not exist", KAFKA_INSTANCE_NAME));
        }

        this.kafkaInstanceApi = KafkaInstanceApiUtils.kafkaInstanceApi(kafka, Environment.PRIMARY_OFFLINE_TOKEN);

        var optionalServiceAccount = SecurityMgmtAPIUtils.getServiceAccountByName(securityMgmtApi, SERVICE_ACCOUNT_NAME);
        if (optionalServiceAccount.isPresent()) {
            serviceAccount = securityMgmtApi.resetServiceAccountCreds(optionalServiceAccount.get().getId());
        } else {
            serviceAccount = SecurityMgmtAPIUtils.applyServiceAccount(securityMgmtApi, SERVICE_ACCOUNT_NAME);
        }

        createACLs(serviceAccount);
        createTopics();

        // initialize map of snapshots for specific queries (metrics)
        PrometheusWebClient.Query queryTrafficIn = new PrometheusWebClient.Query()
            .metric(METRIC_TRAFFIC_IN)
            .label("namespace", String.format("kafka-%s", kafka.getId()))
            .aggregateFunction("sum");
        PrometheusWebClient.Query queryTrafficOut = new PrometheusWebClient.Query()
            .metric(METRIC_TRAFFIC_OUT)
            .label("namespace", String.format("kafka-%s", kafka.getId()))
            .aggregateFunction("sum");
        PrometheusWebClient.Query queryStorageIncrease = new PrometheusWebClient.Query()
            .metric(METRIC_STORAGE)
            .label("namespace", String.format("kafka-%s", kafka.getId()))
            .aggregateFunction("sum");

        metricToSnapshotMap.put(METRIC_TRAFFIC_IN, new PrometheusWebClient.Snapshot(queryTrafficIn));
        metricToSnapshotMap.put(METRIC_TRAFFIC_OUT, new PrometheusWebClient.Snapshot(queryTrafficOut));
        metricToSnapshotMap.put(METRIC_STORAGE, new PrometheusWebClient.Snapshot(queryStorageIncrease));

    }

    @Test(enabled = true)
    @SneakyThrows
    public void testCheckClusterHoursValue() {
        PrometheusWebClient.Query query = new PrometheusWebClient.Query();
        query
            .metric(METRIC_CLUSTER_HOURS)
            .label("_id", kafka.getId());

        QueryResult result = observatoriumClient.query(query);
        Assert.assertEquals(result.status, "success");
        Assert.assertEquals(result.data.result.size(), 1);
        Assert.assertEquals(result.data.result.get(0).value(), "1");
    }

    @Test(enabled = true)
    @SneakyThrows
    public void takeMetricsSnapshot() {
        for (Map.Entry<String, PrometheusWebClient.Snapshot> entryMetricToSnapshot : metricToSnapshotMap.entrySet()) {

            var snapshot = entryMetricToSnapshot.getValue();
            var queryResult = prometheusWebClient.query(snapshot.getQuery());
            log.debug("taking snapshot for metric '{}', with endpoint: '{}'",
                entryMetricToSnapshot.getKey(),
                snapshot.getQuery().toString());
            var observedReturnedValue = queryResult.data.result.get(0).doubleValue();

            snapshot.setObservedValue(observedReturnedValue);
            log.info("setting observed value for metric '{}'", entryMetricToSnapshot.getKey());
        }

        log.info("all snapshots successfully taken");
    }

    @Test(priority = 1, enabled = true)
    @SneakyThrows
    public void invokeDataProduction() {
        String bootstrapHost = kafka.getBootstrapServerHost();
        String clientID = serviceAccount.getClientId();
        String clientSecret = serviceAccount.getClientSecret();

        KafkaProducerClient producer = new KafkaProducerClient(
            Vertx.vertx(),
            bootstrapHost,
            clientID,
            clientSecret,
            KafkaAuthMethod.OAUTH,
            StringSerializer.class,
            StringSerializer.class);

        List<String> producedMessages = KafkaMessagingUtils.generateRandomMessages(this.messageCount, this.messageSize, this.messageSize);
        bwait(producer.sendAsync(TOPIC_NAME, producedMessages));

        producer.close();
    }

    @Test(priority = 2, enabled = true)
    @SneakyThrows
    public void invokeDataConsumption() {
        String bootstrapHost = kafka.getBootstrapServerHost();
        String clientID = serviceAccount.getClientId();
        String clientSecret = serviceAccount.getClientSecret();

        bwait(testTopicWithNConsumers(
            Vertx.vertx(),
            bootstrapHost,
            clientID,
            clientSecret,
            TOPIC_NAME,
            Duration.ofMinutes(3),
            this.messageCount,
            this.messageSize,
            this.consumerCount,
            KafkaAuthMethod.OAUTH));
    }

    @Test(priority = 1, dependsOnMethods = {"invokeDataProduction"}, enabled = true)
    @SneakyThrows
    public void testMetricStorageIncreased() {

        log.info("test correct storage increase metric when data are produced");
        // storage before increasing (value snapshot created even before data were produced)
        double oldStorageTotal = metricToSnapshotMap.get(METRIC_STORAGE).getObservedValue();

        // expected increased value in used space across kafka brokers, i.e, produced bytes (messageSize * messageCount) * number of replicas (3).
        double expectedIncrease = this.messageSize * this.messageCount * 3;
        log.info("expected increase in size: {}", expectedIncrease);

        // waiting for metric to be increased within with 5 range
        KafkaMgmtMetricsUtils.waitUntilExpectedMetricRange(
            prometheusWebClient,
            kafka.getId(),
            metricToSnapshotMap.get(METRIC_STORAGE).getQuery(),
            oldStorageTotal,
            expectedIncrease,
            5.00);
    }

    @Test(priority = 1, dependsOnMethods = {"invokeDataProduction"}, enabled = true)
    @SneakyThrows
    public void testMetricIncomingTrafficIncreased() {

        log.info("test correct incoming traffic metric increase when data are produced");
        // storage before increasing (value snapshot created even before data were produced)
        double oldTrafficInTotal = metricToSnapshotMap.get(METRIC_TRAFFIC_IN).getObservedValue();

        // calculation of expected increased value in metric, i.e, conversion of produced bytes (messages * size).
        double expectedIncrease = this.messageSize * this.messageCount;
        log.info("expected increase in size: {}", expectedIncrease);

        // waiting for metric to be increased within with 5 range
        KafkaMgmtMetricsUtils.waitUntilExpectedMetricRange(
            prometheusWebClient,
            kafka.getId(),
            metricToSnapshotMap.get(METRIC_TRAFFIC_IN).getQuery(),
            oldTrafficInTotal,
            expectedIncrease,
            10);
    }

    @Test(priority = 2, enabled = true)
    @SneakyThrows
    public void testTrafficMetricOutIncreased() {

        log.info("test correct outcoming traffic metric increase when data are consumed");
        // storage before increasing (value snapshot created even before data were produced)
        double oldTrafficOutTotal = metricToSnapshotMap.get(METRIC_TRAFFIC_OUT).getObservedValue();

        // expected increased value in metric, i.e, conversion of produced bytes (messages * size) * number of consumers  + 15% for handshakes + metatada + extra packet size.
        double expectedIncrease = this.messageSize * this.messageCount * this.consumerCount * 1.15;
        log.info("expected increase in size: {}", expectedIncrease);

        // waiting for metric to be increased within with 5 range
        KafkaMgmtMetricsUtils.waitUntilExpectedMetricRange(
            prometheusWebClient,
            kafka.getId(),
            metricToSnapshotMap.get(METRIC_TRAFFIC_OUT).getQuery(),
            oldTrafficOutTotal,
            expectedIncrease,
            10.0);
    }

    @SneakyThrows
    private void createTopics() {
        var topics = kafkaInstanceApi.getTopics();
        for (Topic topic : topics.getItems()) {
            if (topic.getName().equals(TOPIC_NAME)) {
                return;
            }
        }

        // create topic with 3 replicas (the only possible way to create topic currently)
        kafkaInstanceApi.createTopic(
            new NewTopicInput()
                .name(TOPIC_NAME)
                .settings(
                    new TopicSettings().numPartitions(1))
        );
    }

    private void createACLs(ServiceAccount serviceAccount) throws ApiGenericException {
        if (serviceAccount != null) {
            var principal = KafkaInstanceApiAccessUtils.toPrincipal(serviceAccount.getClientId());
            KafkaInstanceApiAccessUtils.createProducerAndConsumerACLs(kafkaInstanceApi, principal);
        }
    }
}
