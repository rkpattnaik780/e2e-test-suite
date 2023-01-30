package io.managed.services.test.kafka;

import com.openshift.cloud.api.kas.models.KafkaRequestPayload;
import io.managed.services.test.Environment;
import io.managed.services.test.client.ApplicationServicesApi;
import io.managed.services.test.client.kafkamgmt.KafkaMgmtApi;
import io.managed.services.test.client.kafkamgmt.KafkaMgmtApiUtils;
import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j2;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static io.managed.services.test.TestUtils.assumeTeardown;
import static java.time.Duration.ofSeconds;
import static org.testng.Assert.assertNotNull;

@Log4j2
public class KafkaRegressionTest {
    static final String KAFKA_INSTANCE_NAME = "mk-e2e-reg-" + Environment.LAUNCH_KEY;

    private KafkaMgmtApi kafkaMgmtApi;

    @BeforeClass
    public void bootstrap() {
        assertNotNull(Environment.PRIMARY_OFFLINE_TOKEN, "the PRIMARY_OFFLINE_TOKEN env is null");

        var apps = ApplicationServicesApi.applicationServicesApi(Environment.PRIMARY_OFFLINE_TOKEN);

        kafkaMgmtApi = apps.kafkaMgmt();
    }

    @AfterClass(alwaysRun = true)
    public void teardown() {
        assumeTeardown();

        try {
            KafkaMgmtApiUtils.cleanKafkaInstance(kafkaMgmtApi, KAFKA_INSTANCE_NAME);
        } catch (Throwable t) {
            log.error("clean second kafka instance error: ", t);
        }
    }

    @Test(priority = 2)
    @SneakyThrows
    public void testDeleteProvisioningKafkaInstance() {

        // Create Kafka Instance
        var payload = new KafkaRequestPayload()
            .name(KAFKA_INSTANCE_NAME)
            .cloudProvider(Environment.CLOUD_PROVIDER)
            .region(Environment.DEFAULT_KAFKA_REGION);

        log.info("create kafka instance '{}'", KAFKA_INSTANCE_NAME);
        var kafkaToDelete = KafkaMgmtApiUtils.createKafkaInstance(kafkaMgmtApi, payload);
        log.debug(kafkaToDelete);

        log.info("wait 3 seconds before start deleting");
        Thread.sleep(ofSeconds(3).toMillis());

        log.info("delete kafka '{}'", kafkaToDelete.getId());
        kafkaMgmtApi.deleteKafkaById(kafkaToDelete.getId(), true);

        log.info("wait for kafka to be deleted '{}'", kafkaToDelete.getId());
        KafkaMgmtApiUtils.waitUntilKafkaIsDeleted(kafkaMgmtApi, kafkaToDelete.getId());
    }
}
