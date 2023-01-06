package io.managed.services.test.cluster;

import com.openshift.cloud.api.kas.models.KafkaRequest;
import com.openshift.cloud.api.kas.models.KafkaRequestPayload;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.fabric8.openshift.client.OpenShiftClient;
import io.managed.services.test.Environment;
import io.managed.services.test.TestBase;
import io.managed.services.test.client.ApplicationServicesApi;
import io.managed.services.test.client.kafkamgmt.KafkaMgmtApi;
import io.managed.services.test.client.kafkamgmt.KafkaMgmtApiUtils;
import io.managed.services.test.k8.managedkafka.resources.v1alpha1.ManagedKafka;
import io.managed.services.test.k8.managedkafka.ManagedKafkaUtils;
import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j2;
import org.testng.Assert;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.ConfigBuilder;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import java.util.Optional;


@Log4j2
public class DeploymentsTest extends TestBase {

    static final String KAFKA_INSTANCE_NAME = "cl-e2e-" + Environment.LAUNCH_KEY;

    private OpenShiftClient oc;

    private static final int MAX_STANDARD_INSTANCES_TOTAL = 7;
    private static final int MAX_DEVELOPER_INSTANCES_TOTAL = 30;

    private KafkaMgmtApi kafkaMgmtApi;

    @BeforeClass
    @SneakyThrows
    public void bootstrap() {

        log.info("build config");
        Config config = new ConfigBuilder()
            .withMasterUrl("https://api.mk-stage-0622.bd59.p1.openshiftapps.com:6443")
            .withOauthToken(Environment.PROMETHEUS_WEB_CLIENT_ACCESS_TOKEN)
            .withTrustCerts(true)
            .build();

        log.info("init openshift client");
        oc = new KubernetesClientBuilder().withConfig(config).build().adapt(OpenShiftClient.class);

        var apps = ApplicationServicesApi.applicationServicesApi(
            Environment.PRIMARY_USERNAME,
            Environment.PRIMARY_PASSWORD);
        kafkaMgmtApi = apps.kafkaMgmt();
    }

    @AfterClass(alwaysRun = true)
    public void teardown() {

        // delete kafka instance
        try {
            KafkaMgmtApiUtils.cleanKafkaInstance(kafkaMgmtApi, KAFKA_INSTANCE_NAME);
        } catch (Throwable t) {
            log.error("clean main kafka instance error: ", t);
        }
    }

    @DataProvider(name = "managedKafkaTypeData")
    public Object[][] managedKafkaTypeData() {
        return new Object[][]{
            {"standard"},
            {"developer"}};
    }

    @Test(dataProvider = "managedKafkaTypeData")
    @SneakyThrows
    public void testReservedDeploymentExistence(String mkType) {

        String namespace = String.format("reserved-kafka-%s-1", mkType);
        log.info("verify existence of reserved deployment in namespace '{}'", namespace);

        String managedKafkaName = String.format("reserved-kafka-%s-1", mkType);
        log.info("verify existence of managed kafka reserved deployment '{}'", managedKafkaName);
        Optional<ManagedKafka> mkOptional = ManagedKafkaUtils.managedKafka(oc).inNamespace(namespace).list().getItems().stream().filter(ManagedKafka::isReserveDeployment).findAny();
        Assert.assertTrue(mkOptional.isPresent(), "reserved deployment is not present");
    }

    @Test
    public void testReservedDeploymentDoesNotPreventKafkaCreation() throws Exception {

        log.info("Obtain information about available quota");
        int standardInstancesCount = ManagedKafkaUtils.getCountOfExistingGivenManagedKafkaType(oc, "standard");
        int developerInstancesCount = ManagedKafkaUtils.getCountOfExistingGivenManagedKafkaType(oc, "developer");
        if (standardInstancesCount > MAX_STANDARD_INSTANCES_TOTAL) {
            log.warn("currently too many standard ({}/{}), or developer ({}/{}) instances",
                standardInstancesCount,
                MAX_STANDARD_INSTANCES_TOTAL,
                developerInstancesCount,
                MAX_DEVELOPER_INSTANCES_TOTAL);
            throw new SkipException("Too many instances, which would cause quota to be breached");
        }

        // we are targeting aws data plane cluster.
        KafkaRequestPayload payload = new KafkaRequestPayload()
            .name(KAFKA_INSTANCE_NAME)
            .cloudProvider("aws")
            .region("us-east-1");

        log.info("attempt to create kafka instance");
        KafkaRequest kafkaRequest = kafkaMgmtApi.createKafka(true, payload);
        log.debug(kafkaRequest);
        KafkaMgmtApiUtils.waitUntilKafkaIsProvisioning(kafkaMgmtApi, kafkaRequest.getId());
    }

}