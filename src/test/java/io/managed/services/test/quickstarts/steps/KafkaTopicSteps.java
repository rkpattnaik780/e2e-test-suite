package io.managed.services.test.quickstarts.steps;

import com.openshift.cloud.api.kas.auth.models.NewTopicInput;
import com.openshift.cloud.api.kas.auth.models.TopicSettings;
import io.cucumber.java.After;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import io.managed.services.test.client.kafkainstance.KafkaInstanceApiUtils;
import io.managed.services.test.quickstarts.contexts.KafkaInstanceContext;
import lombok.extern.log4j.Log4j2;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import static io.managed.services.test.TestUtils.assumeTeardown;
import static org.testng.Assert.assertTrue;

@Log4j2
public class KafkaTopicSteps {

    /**
     * Topics created by this class that will be cleaned in the @After method
     */
    private final Set<String> topicNames = new HashSet<>();

    private final KafkaInstanceContext kafkaInstanceContext;

    public KafkaTopicSteps(KafkaInstanceContext kafkaInstanceContext) {
        this.kafkaInstanceContext = kafkaInstanceContext;
    }

    @When("you create the Kafka topic {word}")
    public void you_create_the_kafka_topic(String name) throws Throwable {
        var kafkaInstance = kafkaInstanceContext.requireKafkaInstance();
        var kafkaInstanceApi = kafkaInstanceContext.kafkaInstanceApi();

        log.info("create topic with name '{}' on kafka instance '{}'", name, kafkaInstance.getName());
        var payload = new NewTopicInput();
        payload.setName(name);
        var settings = new TopicSettings();
        settings.setNumPartitions(1);
        payload.setSettings(settings);
        var topic = kafkaInstanceApi.createTopic(payload);
        log.debug(topic);

        topicNames.add(topic.getName());
    }

    @Given("you have created the Kafka topic {word}")
    public void you_have_created_the_kafka_topic(String name) throws Throwable {
        var kafkaInstance = kafkaInstanceContext.requireKafkaInstance();
        var kafkaInstanceApi = kafkaInstanceContext.kafkaInstanceApi();

        log.info("apply topic with name '{}' on kafka instance '{}'", name, kafkaInstance.getName());
        var payload = new NewTopicInput();
        payload.setName(name);
        var settings = new TopicSettings();
        settings.setNumPartitions(1);
        payload.setSettings(settings);
        var topic = KafkaInstanceApiUtils.applyTopic(kafkaInstanceApi, payload);

        // add to the topics list to clean it in the after method
        topicNames.add(topic.getName());
    }

    @Then("the Kafka topic {word} is listed in the topics table")
    public void the_kafka_topic_is_listed_in_the_topics_table(String name) throws Throwable {
        var kafkaInstanceApi = kafkaInstanceContext.kafkaInstanceApi();

        log.info("get topic list");
        var list = kafkaInstanceApi.getTopics();
        log.debug(list);

        var o = Objects.requireNonNull(list.getItems()).stream()
            .filter(t -> name.equals(t.getName()))
            .findAny();
        assertTrue(o.isPresent());
    }

    @After(order = 10100)
    public void cleanTopics() {

        // nothing to clan
        if (topicNames.isEmpty()) return;

        assumeTeardown();

        var kafkaInstanceApi = kafkaInstanceContext.kafkaInstanceApi();
        for (var topic : topicNames) {
            try {
                log.info("delete topic: {}", topic);
                kafkaInstanceApi.deleteTopic(topic);
            } catch (Throwable t) {
                log.error("clean topic error: ", t);
            }
        }

        topicNames.clear();
    }
}
