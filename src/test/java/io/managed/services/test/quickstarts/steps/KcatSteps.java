package io.managed.services.test.quickstarts.steps;

import io.cucumber.java.After;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import io.managed.services.test.cli.AsyncProcess;
import io.managed.services.test.cli.Kcat;
import io.managed.services.test.quickstarts.contexts.KafkaInstanceContext;
import io.managed.services.test.quickstarts.contexts.ServiceAccountContext;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.io.FileUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

import static io.managed.services.test.TestUtils.assumeTeardown;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

@Log4j2
public class KcatSteps {

    private static final String BINARY = "kcat";

    private static final String BOOTSTRAP_SERVER_ENV = "BOOTSTRAP_SERVER";
    private static final String USER_ENV = "USER";
    private static final String PASSWORD_ENV = "PASSWORD";

    private final KafkaInstanceContext kafkaInstanceContext;
    private final ServiceAccountContext serviceAccountContext;

    private Kcat kcat;

    private AsyncProcess producerProcess;
    private AsyncProcess consumerProcess;

    private boolean exampleMessagesProduced = false;

    public KcatSteps(KafkaInstanceContext kafkaInstanceContext, ServiceAccountContext serviceAccountContext) {
        this.kafkaInstanceContext = kafkaInstanceContext;
        this.serviceAccountContext = serviceAccountContext;
    }

    public Kcat requireKcat() {
        return Objects.requireNonNull(kcat);
    }

    public AsyncProcess requireProducer() {
        return Objects.requireNonNull(producerProcess);
    }

    public AsyncProcess requireConsumer() {
        return Objects.requireNonNull(consumerProcess);
    }

    @Then("bla bla vla")
    public void blaBlaVla() {

        log.info(("dhnsfjjkf dsjjkj"));
    }

    @Given("you have downloaded and verified the latest supported version of Kcat for your operating system")
    public void you_have_downloaded_and_verified_the_latest_supported_version_of_kcat_for_your_operating_system() throws Throwable {

        var workdir = Files.createTempDirectory("kcat");
        var kcat = new Kcat(workdir.toFile(), BINARY);

        var version = kcat.version();
        log.info(version);

        this.kcat = kcat;
    }

    @When("you set the Kafka instance bootstrap server endpoint and service account credentials as environment variables")
    public void you_set_the_kafka_instance_bootstrap_server_endpoint_and_service_account_credentials_as_environment_variables() {
        var kcat = requireKcat();
        var kafkaInstance = kafkaInstanceContext.requireKafkaInstance();
        var serviceAccount = serviceAccountContext.requireServiceAccount();

        kcat.addEnvironment(BOOTSTRAP_SERVER_ENV, kafkaInstance.getBootstrapServerHost());
        kcat.addEnvironment(USER_ENV, serviceAccount.getClientId());
        kcat.addEnvironment(PASSWORD_ENV, serviceAccount.getClientSecret());
    }

    @Given("Kcat is installed")
    public void kcat_is_installed() {
        assertNotNull(kcat);
    }

    @Given("you have set the Kafka bootstrap server endpoint and your service account credentials as environment variables")
    public void you_have_set_the_kafka_bootstrap_server_endpoint_and_your_service_account_credentials_as_environment_variables() {
        var kcat = requireKcat();
        assertNotNull(kcat.getEnvironment(BOOTSTRAP_SERVER_ENV));
        assertNotNull(kcat.getEnvironment(USER_ENV));
        assertNotNull(kcat.getEnvironment(PASSWORD_ENV));
    }

    @When("you start Kcat in producer mode on the topic {word}")
    public void you_start_kcat_in_producer_mode_on_the_topic(String topicName) {
        var kcat = requireKcat();

        log.info("start the kcat producer");
        producerProcess = kcat.startProducer(
            topicName,
            kcat.getEnvironment(BOOTSTRAP_SERVER_ENV),
            kcat.getEnvironment(USER_ENV),
            kcat.getEnvironment(PASSWORD_ENV));

        log.info("kcat producer started");
    }

    @When("you enter messages into Kcat that you want to produce")
    public void you_enter_messages_into_kcat_that_you_want_to_produce() throws Throwable {
        var producer = requireProducer();

        var stdin = producer.stdin();

        log.info("write example messages");
        stdin.write("First message\n");
        stdin.write("Second message\n");
        stdin.write("Third message\n");

        stdin.close();

        exampleMessagesProduced = true;
    }

    @Then("the producer is still running without any errors")
    public void the_producer_is_still_running_without_any_errors() throws Throwable {

        var producer = requireProducer();
        if (producer.isDead()) {
            log.error("the producer is dead");

            // If the process has failed with exit code != 0 it will throw a CliProcessException here
            producer.sync(Duration.ofSeconds(1));

            // otherwise
            throw new Exception("the process is dead but didn't fail", producer.processException(null));
        }
        log.info("the producer is still alive");
    }

    @Given("you used a producer to produce example messages to a topic")
    public void you_used_a_producer_to_produce_example_messages_to_a_topic() {
        assertTrue(exampleMessagesProduced);
    }

    @When("you start Kcat in consumer mode on the topic {word}")
    public void you_start_kcat_in_consumer_mode_on_the_topic(String topicName) {
        var kcat = requireKcat();

        log.info("start the kcat consumer");
        consumerProcess = kcat.startConsumer(
            topicName,
            kcat.getEnvironment(BOOTSTRAP_SERVER_ENV),
            kcat.getEnvironment(USER_ENV),
            kcat.getEnvironment(PASSWORD_ENV));

        log.info("kcat consumer started");
    }

    @Then("your consumer is running without any errors")
    public void your_consumer_is_running_without_any_errors() throws Throwable {

        var consumer = requireConsumer();
        if (consumer.isDead()) {
            log.error("the producer is dead");

            // If the process has failed with exit code != 0 it will throw a CliProcessException here
            consumer.sync(Duration.ofSeconds(1));

            // otherwise
            throw new Exception("the process is dead but didn't fail", consumer.processException(null));
        }
        log.info("the consumer is still alive");
    }

    @Then("the consumer display the example messages from the producer")
    public void the_consumer_display_the_example_messages_from_the_producer() throws Throwable {
        var consumer = requireConsumer();

        var expectedMessages = new ArrayList<String>();
        expectedMessages.add("First message");
        expectedMessages.add("Second message");
        expectedMessages.add("Third message");

        var stdout = consumer.stdout();

        var consumerFuture = consumer.future(Duration.ofMinutes(1));

        var parserFuture = CompletableFuture.runAsync(() -> {
            log.info("start message parser");

            do {
                try {
                    var l = stdout.readLine();
                    if (l == null) {
                        log.info("end of file");
                        break;
                    }

                    log.info("consumer message: {}", l);

                    if (expectedMessages.remove(l)) {
                        log.info("matched message '{}'", l);
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            } while (!expectedMessages.isEmpty());

            log.info("message parser finished");
        });

        CompletableFuture.anyOf(parserFuture, consumerFuture).get();

        log.info("parse messages completed");
        assertEquals(expectedMessages, new ArrayList<>());
    }

    @After(order = 10210)
    public void closeConsumer() {
        if (consumerProcess == null) return;

        try {
            consumerProcess.destroy();
            log.info("consumer output:\n{}", consumerProcess.outputAsString());
        } catch (Throwable t) {
            log.error("close consumer error:", t);
        }

        consumerProcess = null;
    }


    @After(order = 10210)
    public void closeProducer() {
        if (producerProcess == null) return;

        try {
            producerProcess.destroy();
            log.info("producer output:\n{}", producerProcess.outputAsString());
        } catch (Throwable t) {
            log.error("close producer error:", t);
        }

        producerProcess = null;
    }


    @After(order = 10200)
    public void cleanKcatWorkdir() {

        if (kcat == null) return;

        assumeTeardown();

        // clean cli workdir
        log.info("delete workdir: {}", kcat.getWorkdir());
        try {
            FileUtils.deleteDirectory(kcat.getWorkdir());
        } catch (Throwable t) {
            log.error("clean workdir error: ", t);
        }

        kcat = null;
    }
}
