package io.managed.services.test.cluster;

import com.openshift.cloud.api.kas.models.KafkaRequest;
import com.openshift.cloud.api.kas.models.KafkaRequestPayload;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.fabric8.openshift.client.OpenShiftClient;
import io.managed.services.test.Environment;
import io.managed.services.test.TestBase;
import io.managed.services.test.client.ApplicationServicesApi;
import io.managed.services.test.client.exception.ApiGenericException;
import io.managed.services.test.client.kafkamgmt.KafkaMgmtApi;
import io.managed.services.test.client.kafkamgmt.KafkaMgmtApiUtils;
import io.managed.services.test.k8.managedkafka.v1alpha1.ManagedKafka;
import io.managed.services.test.k8.managedkafka.ManagedKafkaUtils;
import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j2;
import org.json.JSONObject;
import org.testng.Assert;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.ConfigBuilder;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import java.util.Optional;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


@Log4j2
public class DeploymentsTest extends TestBase {


    static final String KAFKA_INSTANCE_NAME = "cl-e2e-" + Environment.LAUNCH_SUFFIX;

    private OpenShiftClient oc;

    private static final int MAX_STANDARD_INSTANCES_TOTAL = 7;
    private static final int MAX_DEVELOPER_INSTANCES_TOTAL = 30;

    private static final String KAFKAS_MGMT_21_CODE = "KAFKAS-MGMT-21";
    private static final String KAFKAS_MGMT_21_REASON = "unable to detect instance type in plan provided: ";
    private static final String PLAN_DEVELOPER = "developer.x1";
    private static final String PLAN_STANDARD = "standard.x1";

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

        var apps = ApplicationServicesApi.applicationServicesApi(Environment.PRIMARY_OFFLINE_TOKEN);
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

    @DataProvider(name = "managedKafkaTypes")
    public Object[][] managedKafkaTypeDataProvider() {
        return new Object[][]{
            {"standard"},
            {"developer"}};
    }

    @Test(dataProvider = "managedKafkaTypes")
    @SneakyThrows
    public void testReservedDeploymentExistence(String mkType) {

        String namespace = String.format("reserved-kafka-%s-1", mkType);
        log.info("evaluating namespace '{}'", namespace);

        String managedKafkaName = String.format("reserved-kafka-%s-1", mkType);
        log.info("managed kafka reserved deployment '{}'", managedKafkaName);
        Optional<ManagedKafka> mkOptional = ManagedKafkaUtils.managedKafka(oc).inNamespace(namespace).list().getItems().stream().filter(ManagedKafka::isReserveDeployment).findAny();
        Assert.assertTrue(mkOptional.isPresent(), "reserved deployment is not present");
    }

    @Test(dataProvider = "managedKafkaTypes")
    public void testReservedDeploymentDoesNotPreventKafkaCreation(String mkType) throws Exception {

        log.info("Obtain information about available quota for managed kafka type: '{}'", mkType);

        // obtain max limit of kafka instances per given mk type (developer, standard)
        int upperLimitPerManagedKafkaType;
        switch (mkType) {
            case "standard":
                upperLimitPerManagedKafkaType = MAX_STANDARD_INSTANCES_TOTAL;
                break;
            case "developer":
                upperLimitPerManagedKafkaType = MAX_DEVELOPER_INSTANCES_TOTAL;
                break;
            default:
                throw new Exception("Unsupported managed kafka type");
        }
        log.info("upper limit for instance type '{}' is '{}' instances.", mkType, upperLimitPerManagedKafkaType);

        int currentInstancesCount = ManagedKafkaUtils.getCountOfExistingGivenManagedKafkaType(oc, mkType);
        log.info("currently there are '{}' instances of given type", currentInstancesCount);

        // if there are already max number of instances skip test
        if (currentInstancesCount > upperLimitPerManagedKafkaType) {
            log.warn("currently too many instances ({}/{}), of type '{}'",
                currentInstancesCount,
                upperLimitPerManagedKafkaType,
                mkType);
            throw new SkipException("Too many existing instances, which would cause quota to be breached");
        }

        // create kafka instance of expected type
        KafkaRequestPayload payload = new KafkaRequestPayload()
            .name(KAFKA_INSTANCE_NAME)
            .cloudProvider("aws")
            .region("us-east-1")
            .plan(String.format("%s.x1", mkType));

        log.info("attempt to create kafka instance");
        KafkaRequest kafkaRequest = null;
        try {
            kafkaRequest = kafkaMgmtApi.createKafka(true, payload);
            log.debug(kafkaRequest);
            log.info("wait for provisioning of kafka instance with id '{}'", kafkaRequest.getId());
            KafkaMgmtApiUtils.waitUntilKafkaIsProvisioning(kafkaMgmtApi, kafkaRequest.getId());
        } catch (ApiGenericException e) {
            // some users may not be able to create some types of instances, e.g., user with quota will not be able to create dev. instance
            log.warn(e);
            JSONObject jsonResponse = new JSONObject(e.getResponseBody());
            assertEquals(jsonResponse.get("code"), KAFKAS_MGMT_21_CODE);
            assertTrue(jsonResponse.get("reason").toString().contains(KAFKAS_MGMT_21_REASON));
        } finally {
            // cleanup of kafka instance
            log.info("clean kafka instance with name '{}'", KAFKA_INSTANCE_NAME);
            try {
                KafkaMgmtApiUtils.deleteKafkaByNameIfExists(kafkaMgmtApi, KAFKA_INSTANCE_NAME);
                log.info("wait until kafka is deleted");
                KafkaMgmtApiUtils.waitUntilKafkaIsDeleted(kafkaMgmtApi, kafkaRequest.getId());
            } catch (Exception e) {
                log.error("error while cleaning kafka instance: %s", e);
            }
        }
    }

}