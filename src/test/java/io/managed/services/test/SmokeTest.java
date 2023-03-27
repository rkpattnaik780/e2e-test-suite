package io.managed.services.test;

import com.microsoft.kiota.serialization.JsonParseNodeFactory;
import com.openshift.cloud.api.kas.api.kafkas_mgmt.v1.kafkas.item.metrics.query.MetricsInstantQueryListResponse;
import io.managed.services.test.cli.CliGenericException;
import io.managed.services.test.client.kafka.KafkaMessagingUtils;
import io.managed.services.test.client.kafkamgmt.KafkaMgmtMetricsUtils;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


public class SmokeTest extends TestBase {
    private static final Logger LOGGER = LogManager.getLogger(SmokeTest.class);

    @Test
    public void random() {
        var r1 = KafkaMessagingUtils.random(0, 1);
        LOGGER.info("r1: {}", r1);
        assertTrue(r1 >= 0 && r1 <= 1, "r1 is not in range 0,1");

        var r2 = KafkaMessagingUtils.random(10, 1000);
        LOGGER.info("r2: {}", r2);
        assertTrue(r2 >= 10 && r2 <= 1000, "r2 is not in range 10,1000");

        var r3 = KafkaMessagingUtils.random(10, 10);
        LOGGER.info("r3: {}", r3);
        assertEquals(r3, 10, "r3 is not equal 10");
    }

    @Test
    public void testBlockingWait() throws Throwable {
        var vertx = Vertx.vertx();

        var r = new AtomicReference<IllegalCallerException>();
        var p = Promise.promise();
        vertx.setTimer(10, __ -> {
            try {
                TestUtils.bwait(TestUtils.sleep(vertx, Duration.ofSeconds(1)));
                p.complete();
            } catch (IllegalCallerException e) {
                r.set(e);
                p.complete();
            } catch (Throwable throwable) {
                p.fail(throwable);
            }
        });

        // when used outside a vertx callback bwait should succeed
        TestUtils.bwait(p.future());

        // when used within a vertx callback bwait should fail
        assertNotNull(r.get(), "TestUtils.bwait should throw an IllegalCallerException");
    }

    @Test
    public void parseCode() {
        var code = CliGenericException.parseCode("Refreshing tokens\n" +
            "Tokens refreshed\n" +
            "2021/09/10 10:36:35 \n" +
            "GET /api/kafkas_mgmt/v1/kafkas/sdfihsduifhsdui HTTP/1.1\n" +
            "Host: api.stage.openshift.com\n" +
            "User-Agent: rhoas-cli_0.29.0\n" +
            "Accept: application/json\n" +
            "Accept-Encoding: gzip\n" +
            "\n" +
            "2021/09/10 10:36:36 \n" +
            "HTTP/1.1 200 OK\n" +
            "Cache-Control: private\n" +
            "Content-Type: application/json\n" +
            "Date: Fri, 10 Sep 2021 08:36:36 GMT\n" +
            "Server: envoy\n" +
            "Set-Cookie: sjidfojsd=dsfjsidfoj; path=/; HttpOnly; Secure; SameSite=None\n" +
            "Vary: Authorization\n" +
            "X-Envoy-Upstream-Service-Time: 25\n" +
            "X-Operation-Id: c4thi52eboia8b7fr7s0\n" +
            "\n" +
            "{\"id\":\"sdfsdf\",\"kind\":\"Kafka\",\"href\":\"/api/kafkas_mgmt/v1/kafkas/sdfsdf\",\"status\":\"ready\",\"cloud_provider\":\"aws\",\"multi_az\":true,\"region\":\"us-east-1\",\"owner\":\"dbizzarr\",\"name\":\"cli-e2e-test-instance-change-me\",\"bootstrap_server_host\":\"cli-e-e-te-c-thgbqeboia-b-fr-ha.bf2.kafka-stage.rhcloud.com:443\",\"created_at\":\"2021-09-10T08:32:47.991012Z\",\"updated_at\":\"2021-09-10T08:35:28.053233Z\",\"version\":\"2.7.0\",\"instance_type\":\"eval\"}\n" +
            "Making request to https://admin-fsdsdfds-cli-e-e-te-c-dsfsdf-b-sdfsd-ha.bf2.kafka-stage.rhcloud.com/rest\n" +
            "2021/09/10 10:36:36 \n" +
            "GET /rest/topics/cli-e2e-test-topic HTTP/1.1\n" +
            "Host: admin-server-cli-e-e-te-c-sdfsdf-b-fr-ha.bf2.kafka-stage.rhcloud.com\n" +
            "User-Agent: rhoas-cli_0.29.0\n" +
            "Accept: application/json\n" +
            "Accept-Encoding: gzip\n" +
            "\n" +
            "2021/09/10 10:36:36 \n" +
            "HTTP/1.1 404 Not Found\n" +
            "Content-Length: 121\n" +
            "Content-Type: application/json\n" +
            "Strict-Transport-Security: max-age=31536000\n" +
            "Vary: origin\n" +
            "\n" +
            "{\"code\":404,\"error_message\":\"This server does not host this topic-partition.\",\"class\":\"UnknownTopicOrPartitionException\"}");
        assertEquals(404, code);
    }

    @Test
    public void testCollectTopicMetric() throws IOException {
        var stream = SmokeTest.class.getClassLoader().getResourceAsStream("smoke/user-metrics.json");
        var factory = new JsonParseNodeFactory();
        var parseNode = factory.getParseNode("application/json", stream);
        var metrics = parseNode.getObjectValue(MetricsInstantQueryListResponse::createFromDiscriminatorValue);

        var result = KafkaMgmtMetricsUtils.collectTopicMetric(
            metrics.getItems(),
            "metric-test-topic",
            "kafka_server_brokertopicmetrics_messages_in_total");
        Assert.assertEquals(result, 2856);
    }
}
