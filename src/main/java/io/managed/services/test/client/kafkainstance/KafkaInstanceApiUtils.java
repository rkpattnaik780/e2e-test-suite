package io.managed.services.test.client.kafkainstance;

import com.microsoft.kiota.authentication.BaseBearerTokenAuthenticationProvider;
import com.microsoft.kiota.http.OkHttpRequestAdapter;
import com.openshift.cloud.api.kas.auth.ApiClient;
import com.openshift.cloud.api.kas.auth.models.ConsumerGroup;
import com.openshift.cloud.api.kas.auth.models.NewTopicInput;
import com.openshift.cloud.api.kas.auth.models.Topic;
import com.openshift.cloud.api.kas.auth.models.TopicSettings;
import com.openshift.cloud.api.kas.models.KafkaRequest;
import com.redhat.cloud.kiota.auth.RHAccessTokenProvider;
import io.managed.services.test.Environment;
import io.managed.services.test.IsReady;
import io.managed.services.test.ThrowingFunction;
import io.managed.services.test.ThrowingSupplier;
import io.managed.services.test.client.exception.ApiGenericException;
import io.managed.services.test.client.exception.ApiNotFoundException;
import io.managed.services.test.client.kafka.KafkaAuthMethod;
import io.managed.services.test.client.kafka.KafkaConsumerClient;
import io.managed.services.test.wait.TReadyFunction;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.javatuples.Pair;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static io.managed.services.test.TestUtils.waitFor;
import static java.time.Duration.ofMinutes;
import static java.time.Duration.ofSeconds;

@Log4j2
public class KafkaInstanceApiUtils {

    public static String kafkaInstanceApiUri(KafkaRequest kafka) {
        return kafkaInstanceApiUri(kafka.getBootstrapServerHost());
    }

    public static String kafkaInstanceApiUri(String bootstrapServerHost) {
        int portIndex = bootstrapServerHost.indexOf(':');
        String hostname = portIndex >= 0 ? bootstrapServerHost.substring(0, portIndex) : bootstrapServerHost;
        String uriTemplate = Environment.KAFKA_INSTANCE_API_TEMPLATE;
        return String.format(uriTemplate, hostname);
    }

    public static KafkaInstanceApi kafkaInstanceApi(KafkaRequest kafka, String offlineToken) {
        return kafkaInstanceApi(kafkaInstanceApiUri(kafka), offlineToken);
    }

    public static KafkaInstanceApi kafkaInstanceApi(String uri, String offlineToken) {
        var adapter = new OkHttpRequestAdapter(new BaseBearerTokenAuthenticationProvider(new RHAccessTokenProvider(offlineToken)));
        adapter.setBaseUrl(uri);
        ApiClient client = new ApiClient(adapter);

        //  TODO: implement case for situation of insecure connection is necessary


        return new KafkaInstanceApi(client);
    }

    public static Future<KafkaConsumerClient<String, String>> startConsumerGroup(
        Vertx vertx,
        String consumerGroup,
        String topicName,
        String bootstrapServerHost,
        String clientId,
        String clientSecret) {

        log.info("start kafka consumer with group id '{}'", consumerGroup);
        var consumer = new KafkaConsumerClient<>(vertx,
            bootstrapServerHost,
            clientId,
            clientSecret,
            KafkaAuthMethod.OAUTH,
            consumerGroup,
            "latest",
            StringDeserializer.class,
            StringDeserializer.class);

        log.info("subscribe to topic '{}'", topicName);
        consumer.subscribe(topicName);
        consumer.handler(r -> {
            // ignore
        });

        IsReady<Void> subscribed = last -> consumer.assignment().map(partitions -> {
            var o = partitions.stream().filter(p -> p.getTopic().equals(topicName)).findAny();
            return Pair.with(o.isPresent(), null);
        });
        return waitFor(vertx, "consumer group to subscribe", ofSeconds(2), ofMinutes(2), subscribed)
            .map(__ -> consumer);
    }

    public static Optional<ConsumerGroup> getConsumerGroupByName(KafkaInstanceApi api, String consumerGroupId)
        throws ApiGenericException {

        try {
            return Optional.of(api.getConsumerGroupById(consumerGroupId));
        } catch (ApiNotFoundException e) {
            return Optional.empty();
        }
    }

    public static ConsumerGroup waitForConsumerGroup(KafkaInstanceApi api, String consumerGroupId)
        throws ApiGenericException, InterruptedException, TimeoutException {

        return waitForConsumerGroup(() -> getConsumerGroupByName(api, consumerGroupId));
    }

    public static <T extends Throwable> ConsumerGroup waitForConsumerGroup(
        ThrowingSupplier<Optional<ConsumerGroup>, T> supplier)
        throws T, InterruptedException, TimeoutException {

        var groupAtom = new AtomicReference<ConsumerGroup>();
        ThrowingFunction<Boolean, Boolean, T> ready = last -> {
            var exists = supplier.get();
            if (exists.isPresent()) {
                groupAtom.set(exists.get());
                return true;
            }
            return false;
        };

        // wait for the consumer group to show at least one consumer
        // because it could take a few seconds for the kafka admin api to
        // report the connected consumer
        waitFor("consumer group", ofSeconds(2), ofMinutes(1), ready);

        return groupAtom.get();
    }

    public static ConsumerGroup waitForConsumersInConsumerGroup(KafkaInstanceApi api, String consumerGroupId)
        throws ApiGenericException, InterruptedException, TimeoutException {

        TReadyFunction<ConsumerGroup, ApiGenericException> ready = (last, atom) -> {
            var group = api.getConsumerGroupById(consumerGroupId);
            atom.set(group);
            return group.getConsumers().size() > 0;
        };

        // wait for the consumer group to show at least one consumer
        // because it could take a few seconds for the kafka admin api to
        // report the connected consumer
        return waitFor("consumers in consumer group", ofSeconds(2), ofMinutes(1), ready);
    }

    public static Optional<Topic> getTopicByName(KafkaInstanceApi api, String name) throws ApiGenericException {
        try {
            return Optional.of(api.getTopic(name));
        } catch (ApiGenericException e) {
            return Optional.empty();
        }
    }

    public static Topic applyTopic(KafkaInstanceApi api, String topicName) throws ApiGenericException {
        var topicPayload = new NewTopicInput();
        topicPayload.setName(topicName);
        var settings = new TopicSettings();
        settings.setNumPartitions(1);
        topicPayload.setSettings(settings);
        return applyTopic(api, topicPayload);
    }

    public static Topic applyTopic(KafkaInstanceApi api, NewTopicInput payload) throws ApiGenericException {
        var existing = getTopicByName(api, payload.getName());

        if (existing.isPresent()) {
            var topic = existing.get();
            log.warn("topic '{}' already exists", topic.getName());
            log.debug(topic);
            return topic;
        } else {
            log.info("create topic '{}'", payload.getName());
            var topic = api.createTopic(payload);
            log.debug(topic);
            return topic;
        }
    }

    public static Topic updateTopicPartition(KafkaInstanceApi api, String name, int partitions) throws ApiGenericException {
        TopicSettings topicSettings = new TopicSettings();
        topicSettings.setNumPartitions(partitions);
        return api.updateTopic(name, topicSettings);
    }

    // only partitions from public topic are visible (internal and redhat topic are not included, e.g. __consumer_offsets, __redhat_* )
    public static int getPartitionCountTotal(KafkaInstanceApi api) throws ApiGenericException {
        return api.getTopics().getItems().stream().mapToInt(t -> Objects.requireNonNull(t.getPartitions()).size()).reduce(0, Integer::sum);
    }
}
