package io.managed.services.test.registry;


import com.openshift.cloud.api.kas.models.KafkaRequest;
import com.openshift.cloud.api.kas.models.ServiceAccount;
import com.openshift.cloud.api.srs.models.RootTypeForRegistry;
import io.apicurio.registry.rest.v2.beans.RoleMapping;
import io.apicurio.registry.serde.SerdeConfig;
import io.apicurio.registry.types.RoleType;
import io.managed.services.test.Environment;
import io.managed.services.test.TestBase;
import io.managed.services.test.client.ApplicationServicesApi;
import io.managed.services.test.client.kafka.AvroKafkaGenericDeserializer;
import io.managed.services.test.client.kafka.AvroKafkaGenericSerializer;
import io.managed.services.test.client.kafka.KafkaAuthMethod;
import io.managed.services.test.client.kafka.KafkaConsumerClient;
import io.managed.services.test.client.kafka.KafkaProducerClient;
import io.managed.services.test.client.kafkainstance.KafkaInstanceApiAccessUtils;
import io.managed.services.test.client.kafkainstance.KafkaInstanceApiUtils;
import io.managed.services.test.client.kafkamgmt.KafkaMgmtApi;
import io.managed.services.test.client.kafkamgmt.KafkaMgmtApiUtils;
import io.managed.services.test.client.registrymgmt.RegistryMgmtApi;
import io.managed.services.test.client.registrymgmt.RegistryMgmtApiUtils;
import io.managed.services.test.client.securitymgmt.SecurityMgmtAPIUtils;
import io.managed.services.test.client.securitymgmt.SecurityMgmtApi;
import io.vertx.core.Vertx;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Date;
import java.util.HashMap;
import java.util.List;

import static io.managed.services.test.TestUtils.assumeTeardown;
import static io.managed.services.test.TestUtils.bwait;
import static io.managed.services.test.client.kafkainstance.KafkaInstanceApiUtils.kafkaInstanceApiUri;
import static io.managed.services.test.client.registry.RegistryClientUtils.registryClient;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

/**
 * Test integration between Kafka and Service Registry
 * <p>
 * <b>Requires:</b>
 * <ul>
 *     <li> PRIMARY_OFFLINE_TOKEN
 * </ul>
 */
public class RegistryKafkaIntegrationTest extends TestBase {
    private static final Logger LOGGER = LogManager.getLogger(RegistryKafkaIntegrationTest.class);

    private static final String KAFKA_INSTANCE_NAME = "mk-e2e-ki-rki-"  + Environment.LAUNCH_SUFFIX;
    private static final String SERVICE_REGISTRY_NAME = "mk-e2e-sr-rki-"  + Environment.LAUNCH_SUFFIX;
    private static final String SERVICE_ACCOUNT_NAME = "mk-e2e-sa-rki-"  + Environment.LAUNCH_SUFFIX;
    private static final String TOPIC_NAME = "test-topic";
    private static final String ARTIFACT_SCHEMA = "{\"type\":\"record\",\"name\":\"Greeting\",\"fields\":[{\"name\":\"Message\",\"type\":\"string\"},{\"name\":\"Time\",\"type\":\"long\"}]}";

    private final Vertx vertx = Vertx.vertx();

    private RegistryMgmtApi registryMgmtApi;
    private RootTypeForRegistry registry;
    private KafkaMgmtApi kafkaMgmtApi;
    private SecurityMgmtApi securityMgmtApi;
    private KafkaRequest kafka;
    private ServiceAccount serviceAccount;
    private KafkaProducerClient<String, GenericRecord> producer;
    private KafkaConsumerClient<String, GenericRecord> consumer;

    @BeforeClass
    public void bootstrap() throws Throwable {
        assertNotNull(Environment.PRIMARY_OFFLINE_TOKEN, "the OFFLINE_TOKEN env is null");

        var offlineToken = Environment.PRIMARY_OFFLINE_TOKEN;

        // registry api
        LOGGER.info("initialize registry, kafka security services apis");
        var apis = ApplicationServicesApi.applicationServicesApi(offlineToken);
        registryMgmtApi = apis.registryMgmt();
        kafkaMgmtApi = apis.kafkaMgmt();
        securityMgmtApi = apis.securityMgmt();

        // registry
        LOGGER.info("create service registry: {}", SERVICE_REGISTRY_NAME);
        registry = RegistryMgmtApiUtils.applyRegistry(registryMgmtApi, SERVICE_REGISTRY_NAME);

        // kafka
        LOGGER.info("create kafka instance: {}", KAFKA_INSTANCE_NAME);
        kafka = KafkaMgmtApiUtils.applyKafkaInstance(kafkaMgmtApi, KAFKA_INSTANCE_NAME);
        LOGGER.debug(kafka);

        // service account
        LOGGER.info("create service account: {}", SERVICE_ACCOUNT_NAME);
        serviceAccount = SecurityMgmtAPIUtils.applyServiceAccount(securityMgmtApi, SERVICE_ACCOUNT_NAME);

        // topic
        LOGGER.info("create topic: {}", TOPIC_NAME);
        var kafkaInstanceApi = KafkaInstanceApiUtils.kafkaInstanceApi(kafkaInstanceApiUri(kafka), offlineToken);
        var topic = KafkaInstanceApiUtils.applyTopic(kafkaInstanceApi, TOPIC_NAME);
        LOGGER.debug(topic);

        // grant access to the service account to the kafka instance
        LOGGER.info("grant access to the kafka instance for service account: {}", serviceAccount.getClientId());
        KafkaInstanceApiAccessUtils.createProducerAndConsumerACLs(kafkaInstanceApi, KafkaInstanceApiAccessUtils.toPrincipal(serviceAccount.getClientId()));

        // grant access to the service account to the registry
        LOGGER.info("grant access to the registry for service account: {}", serviceAccount.getClientId());
        var registryClient = registryClient(registry.getRegistryUrl(), offlineToken);
        var role = new RoleMapping();
        // We expect the service account to be always created with the same name
        // if that will not be the case in the feature we will have to retrieve the
        // service account name separately
        role.setPrincipalId("service-account-" + serviceAccount.getClientId());
        role.setRole(RoleType.DEVELOPER);
        registryClient.createRoleMapping(role);
    }

    @AfterClass(alwaysRun = true)
    public void teardown() throws Throwable {
        assumeTeardown();

        try {
            KafkaMgmtApiUtils.cleanKafkaInstance(kafkaMgmtApi, KAFKA_INSTANCE_NAME);
        } catch (Throwable t) {
            LOGGER.error("clan kafka error: ", t);
        }

        try {
            SecurityMgmtAPIUtils.cleanServiceAccount(securityMgmtApi, SERVICE_ACCOUNT_NAME);
        } catch (Throwable t) {
            LOGGER.error("clean service account error: ", t);
        }

        try {
            RegistryMgmtApiUtils.cleanRegistry(registryMgmtApi, SERVICE_REGISTRY_NAME);
        } catch (Throwable t) {
            LOGGER.error("clean service registry error: ", t);
        }

        try {
            bwait(consumer.asyncClose());
        } catch (Throwable t) {
            LOGGER.error("close consumer error: ", t);
        }

        try {
            bwait(producer.asyncClose());
        } catch (Throwable t) {
            LOGGER.error("close consumer error: ", t);
        }

        bwait(vertx.close());
    }

    @Test
    public void testProduceConsumeAvroMessageWithServiceRegistry() throws Throwable {

        // producer
        LOGGER.info("initialize producer with registry");
        var producerRegistryConfig = new HashMap<String, String>();
        producerRegistryConfig.put(SerdeConfig.REGISTRY_URL, registry.getRegistryUrl());
        producerRegistryConfig.put(SerdeConfig.AUTO_REGISTER_ARTIFACT, "true");
        producerRegistryConfig.put(SerdeConfig.AUTO_REGISTER_ARTIFACT_IF_EXISTS, "RETURN");
        producerRegistryConfig.put(SerdeConfig.AUTH_USERNAME, serviceAccount.getClientId());
        producerRegistryConfig.put(SerdeConfig.AUTH_PASSWORD, serviceAccount.getClientSecret());

        producer = new KafkaProducerClient<>(
            vertx,
            kafka.getBootstrapServerHost(),
            serviceAccount.getClientId(),
            serviceAccount.getClientSecret(),
            KafkaAuthMethod.OAUTH,
            StringSerializer.class,
            AvroKafkaGenericSerializer.class,
            producerRegistryConfig);

        // consumer
        LOGGER.info("initialize consumer with registry");
        var consumerRegistryConfig = new HashMap<String, String>();
        consumerRegistryConfig.put(SerdeConfig.REGISTRY_URL, registry.getRegistryUrl());
        consumerRegistryConfig.put(SerdeConfig.AUTH_USERNAME, serviceAccount.getClientId());
        consumerRegistryConfig.put(SerdeConfig.AUTH_PASSWORD, serviceAccount.getClientSecret());

        consumer = new KafkaConsumerClient<>(
            vertx,
            kafka.getBootstrapServerHost(),
            serviceAccount.getClientId(),
            serviceAccount.getClientSecret(),
            KafkaAuthMethod.OAUTH,
            "test-group",
            "latest",
            StringDeserializer.class,
            AvroKafkaGenericDeserializer.class,
            consumerRegistryConfig);

        var schema = new Schema.Parser().parse(ARTIFACT_SCHEMA);

        LOGGER.info("prepare the record to send");
        var record = new GenericData.Record(schema);
        var now = new Date();
        record.put("Message", "Hello World");
        record.put("Time", now.getTime());

        LOGGER.info("start the consumer");
        var futureRecords = bwait(consumer.receiveAsync(TOPIC_NAME, 1));

        LOGGER.info("produce the record");
        bwait(producer.sendAsync(TOPIC_NAME, List.of(record)));

        LOGGER.info("consumer the record");
        var records = bwait(futureRecords);

        LOGGER.info("Records:");
        for (var r : records) {
            LOGGER.info("  K: {}; V: {}", r.record().key(), r.record().value().get("Message"));
        }

        var m = (Utf8) records.get(0).record().value().get("Message");
        assertEquals(m.toString(), "Hello World");
    }
}
