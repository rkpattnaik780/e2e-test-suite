package io.managed.services.test.kafka;

import com.openshift.cloud.api.kas.models.KafkaRequest;
import io.managed.services.test.Environment;
import io.managed.services.test.TestBase;
import io.managed.services.test.client.openshiftapiadmin.OpenshiftAdminEndpointApi;
import io.managed.services.test.client.kafkainstance.KafkaInstanceApi;
import io.managed.services.test.client.kafkainstance.KafkaInstanceApiUtils;
import io.managed.services.test.client.kafkamgmt.KafkaMgmtApi;
import io.managed.services.test.client.kafkamgmt.KafkaMgmtApiUtils;
import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j2;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static io.managed.services.test.TestUtils.assumeTeardown;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

@Log4j2
public class KafkaOverseeingTest extends TestBase {

    static final String KAFKA_INSTANCE_NAME = "mk-e2e-ow-" + Environment.LAUNCH_SUFFIX;
    private KafkaRequest kafka;
    private KafkaInstanceApi kafkaInstanceApi;
    private KafkaMgmtApi kafkaMgmtApi;
    private OpenshiftAdminEndpointApi adminEndpointApi;

    @BeforeClass
    @SneakyThrows
    public void bootstrap() {

        assertNotNull(Environment.PRIMARY_OFFLINE_TOKEN, "the PRIMARY_OFFLINE_TOKEN env is null");
        assertNotNull(Environment.STAGE_DATA_PLANE_ADMIN_CLIENT_ID, "the STAGE_DATA_PLANE_ADMIN_CLIENT_ID env is null");
        assertNotNull(Environment.STAGE_DATA_PLANE_ADMIN_CLIENT_SECRET, "the STAGE_DATA_PLANE_ADMIN_CLIENT_SECRET env is null");

        // initialize the mgmt APIs
        kafkaMgmtApi = KafkaMgmtApiUtils.kafkaMgmtApi(Environment.OPENSHIFT_API_URI, Environment.PRIMARY_OFFLINE_TOKEN);

        log.info("create kafka instance '{}'", KAFKA_INSTANCE_NAME);
        kafka = KafkaMgmtApiUtils.applyKafkaInstance(kafkaMgmtApi, KAFKA_INSTANCE_NAME);
        kafkaInstanceApi = KafkaInstanceApiUtils.kafkaInstanceApi(kafka, Environment.PRIMARY_OFFLINE_TOKEN);

        // initialize client to communicate with admin endpoint on openshift uri.
        adminEndpointApi = new OpenshiftAdminEndpointApi(
            Environment.STAGE_DATA_PLANE_ADMIN_CLIENT_ID,
            Environment.STAGE_DATA_PLANE_ADMIN_CLIENT_SECRET
        );
    }

    @AfterClass(alwaysRun = true)
    @SneakyThrows
    public void teardown() {

        assumeTeardown();
        // clean kafka instance
        try {
            KafkaMgmtApiUtils.cleanKafkaInstance(this.kafkaMgmtApi, KAFKA_INSTANCE_NAME);
        } catch (Throwable t) {
            log.error("clean main kafka instance error: ", t);
        }
    }

    @SneakyThrows
    @Test
    public void testSuspendKafkaInstance() {

        log.info("kafka instance with name'{}', and id '{}' is to be suspended", kafka.getName(), kafka.getId());
        this.adminEndpointApi.patchKafkaInstanceSuspension(kafka.getId(), true);

        log.info("wait until kafka status is stated to be suspended");
        String newKafkaState = KafkaMgmtApiUtils.waitUntilKafkaIsSuspended(this.kafkaMgmtApi, this.kafka.getId());
        assertEquals(newKafkaState, "suspended");
    }

    @SneakyThrows
    @Test(dependsOnMethods = "testSuspendKafkaInstance")
    public void testResumeKafkaInstance() {

        log.info("kafka instance with name'{}', and id '{}' is to be resumed", kafka.getName(), kafka.getId());
        this.adminEndpointApi.patchKafkaInstanceSuspension(kafka.getId(), false);

        // validate that status is suspended, which should be immediate
        log.info("wait until kafka status is stated to be suspended");
        String newKafkaState = KafkaMgmtApiUtils.waitUntilKafkaIsResumed(this.kafkaMgmtApi, this.kafka.getId());
        assertEquals(newKafkaState, "ready");

        log.info("try to call kafka instance api after kafka instance is resumed");
        kafkaInstanceApi.getTopics();
    }
}
