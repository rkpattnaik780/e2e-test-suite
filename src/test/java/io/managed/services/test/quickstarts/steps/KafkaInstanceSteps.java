package io.managed.services.test.quickstarts.steps;

import io.cucumber.java.AfterAll;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import io.managed.services.test.Environment;
import io.managed.services.test.client.kafkamgmt.KafkaMgmtApiUtils;
import io.managed.services.test.quickstarts.contexts.KafkaInstanceContext;
import io.managed.services.test.quickstarts.contexts.OpenShiftAPIContext;
import lombok.extern.log4j.Log4j2;

import static io.managed.services.test.TestUtils.assumeTeardown;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

@Log4j2
public class KafkaInstanceSteps {

    private static final String KAFKA_INSTANCE_UNIQUE_NAME = "cucumber-qs-" + Environment.LAUNCH_KEY;

    private final OpenShiftAPIContext openShiftAPIContext;
    private final KafkaInstanceContext kafkaInstanceContext;

    public KafkaInstanceSteps(OpenShiftAPIContext openShiftAPIContext, KafkaInstanceContext kafkaInstanceContext) {
        this.openShiftAPIContext = openShiftAPIContext;
        this.kafkaInstanceContext = kafkaInstanceContext;
    }

    @When("you create a Kafka instance with a unique name")
    public void you_create_a_kafka_instance_with_a_unique_name() throws Throwable {
        var kafkaMgmtApi = openShiftAPIContext.requireKafkaMgmtApi();

        log.info("create kafka instance with name '{}'", KAFKA_INSTANCE_UNIQUE_NAME);
        var payload = KafkaMgmtApiUtils.defaultKafkaInstance(KAFKA_INSTANCE_UNIQUE_NAME);
        var kafka = KafkaMgmtApiUtils.createKafkaInstance(kafkaMgmtApi, payload);
        log.debug(kafka);

        kafkaInstanceContext.setKafkaInstance(kafka);
    }

    @Then("the Kafka instance is listed in the instances table")
    public void the_kafka_instance_is_listed_in_the_instances_table() throws Throwable {
        var kafkaMgmtApi = openShiftAPIContext.requireKafkaMgmtApi();

        var list = kafkaMgmtApi.getKafkas(null, null, null, null);
        log.debug(list);

        var o = list.getItems().stream()
            .filter(k -> KAFKA_INSTANCE_UNIQUE_NAME.equals(k.getName()))
            .findAny();
        assertTrue(o.isPresent());
    }

    @Then("the Kafka instance is shown as Ready")
    public void the_kafka_instance_is_shown_as_ready() throws Throwable {
        var kafkaMgmtApi = openShiftAPIContext.requireKafkaMgmtApi();
        var instanceId = kafkaInstanceContext.requireKafkaInstance().getId();

        var instance = KafkaMgmtApiUtils.waitUntilKafkaIsReady(kafkaMgmtApi, instanceId);
        log.debug(instance);

        assertEquals(instance.getStatus(), "ready");

        kafkaInstanceContext.setKafkaInstance(instance);
    }

    @Given("you have a running Kafka instance in OpenShift Streams for Apache Kafka")
    public void you_have_a_running_kafka_instance_in_open_shift_streams_for_apache_kafka() throws Throwable {
        var kafkaMgmtApi = openShiftAPIContext.requireKafkaMgmtApi();

        log.info("load kafka instance with name '{}'", KAFKA_INSTANCE_UNIQUE_NAME);
        var payload = KafkaMgmtApiUtils.defaultKafkaInstance(KAFKA_INSTANCE_UNIQUE_NAME);
        var kafkaInstance = KafkaMgmtApiUtils.applyKafkaInstance(kafkaMgmtApi, payload);
        log.debug(kafkaInstance);

        kafkaInstanceContext.setKafkaInstance(kafkaInstance);
    }

    @Given("you have created a Kafka instance in OpenShift Streams for Apache Kafka")
    public void you_ve_created_a_kafka_instance_in_open_shift_streams_for_apache_kafka() {
        assertNotNull(kafkaInstanceContext.getKafkaInstance());
    }

    @Given("the Kafka instance is in Ready state")
    public void the_kafka_instance_is_in_ready_state() {
        var kafkaInstance = kafkaInstanceContext.requireKafkaInstance();
        assertEquals(kafkaInstance.getStatus(), "ready");
    }

    @Given("you have the bootstrap server endpoint for your Kafka instance")
    @Given("the Kafka instance has a generated bootstrap server")
    public void the_kafka_instance_has_a_generated_bootstrap_server() {
        var kafkaInstance = kafkaInstanceContext.requireKafkaInstance();
        assertNotNull(kafkaInstance.getBootstrapServerHost());
    }

    @AfterAll
    public static void clean_kafka_instance() throws Throwable {
        assumeTeardown();

        var kafkaMgmtApi = KafkaMgmtApiUtils.kafkaMgmtApi(Environment.OPENSHIFT_API_URI, Environment.PRIMARY_OFFLINE_TOKEN);

        log.info("clean kafka instance: {}", KAFKA_INSTANCE_UNIQUE_NAME);
        KafkaMgmtApiUtils.cleanKafkaInstance(kafkaMgmtApi, KAFKA_INSTANCE_UNIQUE_NAME);
    }
}
