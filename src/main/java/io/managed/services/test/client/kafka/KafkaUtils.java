package io.managed.services.test.client.kafka;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;


public class KafkaUtils {
    private static final Logger LOGGER = LogManager.getLogger(KafkaUtils.class);



    static public <T> CompletionStage<T> toCompletionStage(KafkaFuture<T> future) {
        CompletableFuture<T> completable = new CompletableFuture<>();
        future.whenComplete((r, e) -> {
            if (e == null) {
                completable.complete(r);
            } else {
                completable.completeExceptionally(e);
            }
        });
        return completable;
    }

    static public <T> Future<T> toVertxFuture(KafkaFuture<T> future) {
        Promise<T> promise = Promise.promise();

        future.whenComplete((r, e) -> {
            if (e == null) {
                promise.complete(r);
            } else {
                promise.fail(e);
            }
        });
        return promise.future();
    }

    static public Future<Optional<TopicDescription>> getTopicByName(KafkaAdmin admin, String name) {
        return admin.getMapOfTopicNameAndDescriptionByName(name)
                .map(r -> r.get(name))
                .recover(t -> {
                    LOGGER.error("topic not found:", t);
                    return Future.succeededFuture(null);
                })
                .map(Optional::ofNullable);
    }
}
