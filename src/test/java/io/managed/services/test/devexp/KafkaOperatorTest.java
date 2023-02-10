package io.managed.services.test.devexp;


import com.redhat.rhoas.v1alpha1.CloudServiceAccountRequest;
import com.redhat.rhoas.v1alpha1.CloudServiceAccountRequestSpec;
import com.redhat.rhoas.v1alpha1.CloudServicesRequest;
import com.redhat.rhoas.v1alpha1.CloudServicesRequestSpec;
import com.redhat.rhoas.v1alpha1.KafkaConnectionBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.openshift.client.DefaultOpenShiftClient;
import io.fabric8.openshift.client.OpenShiftClient;
import io.managed.services.test.Environment;
import io.managed.services.test.TestBase;
import io.managed.services.test.TestUtils;
import io.managed.services.test.client.kafkamgmt.KafkaMgmtApi;
import io.managed.services.test.client.kafkamgmt.KafkaMgmtApiUtils;
import io.managed.services.test.client.securitymgmt.SecurityMgmtAPIUtils;
import io.managed.services.test.client.securitymgmt.SecurityMgmtApi;
import io.managed.services.test.framework.LogCollector;
import io.managed.services.test.operator.OperatorUtils;
import io.managed.services.test.wait.ReadyFunction;
import io.vertx.core.json.Json;
import lombok.SneakyThrows;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.ITestContext;
import org.testng.TestException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Base64;
import java.util.HashMap;

import static io.managed.services.test.TestUtils.assumeTeardown;
import static io.managed.services.test.TestUtils.message;
import static io.managed.services.test.TestUtils.waitFor;
import static java.time.Duration.ofMinutes;
import static java.time.Duration.ofSeconds;
import static org.testng.Assert.assertNotNull;


/**
 * Test the application services operator[1] kafka operations.
 * <p>
 * The tests expect the operator to be already installed on the dev cluster, the dev cluster is given by
 * the DEV_CLUSTER_SERVER env. The tested CRs will be created in the DEV_CLUSTER_NAMESPACE set namespace.
 * <p>
 * The DEV_CLUSTER_NAMESPACE can and the DEV_CLUSTER_TOKEN should be created using
 * the ./hack/bootstrap-mk-e2e-tests-namespace.sh script.
 * <p>
 * 1. https://github.com/redhat-developer/app-services-operator
 * <p>
 * <b>Requires:</b>
 * <ul>
 *     <li> PRIMARY_OFFLINE_TOKEN
 *     <li> DEV_CLUSTER_SERVER
 *     <li> DEV_CLUSTER_TOKEN
 * </ul>
 */
public class KafkaOperatorTest extends TestBase {
    private static final Logger LOGGER = LogManager.getLogger(KafkaOperatorTest.class);

    private KafkaMgmtApi kafkaMgmtApi;
    private SecurityMgmtApi securityMgmtApi;
    private OpenShiftClient oc;

    private CloudServicesRequest cloudServicesRequest;

    private static final String KAFKA_INSTANCE_NAME = "mk-e2e-ko-"  + Environment.LAUNCH_SUFFIX;
    private final static String ACCESS_TOKEN_SECRET_NAME = "mk-e2e-api-accesstoken";
    private final static String CLOUD_SERVICE_ACCOUNT_REQUEST_NAME = "mk-e2e-service-account-request";
    private final static String SERVICE_ACCOUNT_NAME = "mk-e2e-bo-sa-"  + Environment.LAUNCH_SUFFIX;
    private final static String SERVICE_ACCOUNT_SECRET_NAME = "mk-e2e-service-account-secret";
    private final static String CLOUD_SERVICES_REQUEST_NAME = "mk-e2e-kafka-request";
    private final static String KAFKA_CONNECTION_NAME = "mk-e2e-kafka-connection";

    @BeforeClass
    @SneakyThrows
    public void bootstrap() {
        assertNotNull(Environment.PRIMARY_OFFLINE_TOKEN, "the PRIMARY_OFFLINE_TOKEN env is null");
        assertNotNull(Environment.DEV_CLUSTER_SERVER, "the DEV_CLUSTER_SERVER env is null");
        assertNotNull(Environment.DEV_CLUSTER_TOKEN, "the DEV_CLUSTER_TOKEN env is null");

        LOGGER.info("initialize kafka and security apis");
        kafkaMgmtApi = KafkaMgmtApiUtils.kafkaMgmtApi(Environment.OPENSHIFT_API_URI, Environment.PRIMARY_OFFLINE_TOKEN);
        securityMgmtApi = SecurityMgmtAPIUtils.securityMgmtApi(Environment.OPENSHIFT_API_URI, Environment.PRIMARY_OFFLINE_TOKEN);


        LOGGER.info("initialize openshift client");
        var config = new ConfigBuilder()
            .withMasterUrl(Environment.DEV_CLUSTER_SERVER)
            .withOauthToken(Environment.DEV_CLUSTER_TOKEN)
            .withNamespace(Environment.DEV_CLUSTER_NAMESPACE)
            .withTrustCerts(true)
            .build();
        oc = new DefaultOpenShiftClient(config);

        LOGGER.info("create kafka instance '{}'", KAFKA_INSTANCE_NAME);
        KafkaMgmtApiUtils.applyKafkaInstance(kafkaMgmtApi, KAFKA_INSTANCE_NAME);

        try {
            OperatorUtils.patchTheOperatorCloudServiceAPIEnv(oc);
        } catch (Throwable t) {
            LOGGER.error("failed to patch the CLOUD_SERVICES_API env:", t);
        }
    }

    private void cleanAccessTokenSecret() {
        Secret s = oc.secrets().withName(ACCESS_TOKEN_SECRET_NAME).get();
        if (s != null) {
            LOGGER.info("clean secret: {}", s.getMetadata().getName());
            oc.secrets().delete(s);
        }
    }

    private void cleanCloudServiceAccountRequest() {
        var a = OperatorUtils.cloudServiceAccountRequest(oc).withName(CLOUD_SERVICE_ACCOUNT_REQUEST_NAME).get();
        if (a != null) {
            LOGGER.info("clean CloudServiceAccountRequest: {}", a.getMetadata().getName());
            OperatorUtils.cloudServiceAccountRequest(oc).delete(a);
        }
    }

    private void cleanCloudServicesRequest() {
        var k = OperatorUtils.cloudServicesRequest(oc).withName(CLOUD_SERVICES_REQUEST_NAME).get();
        if (k != null) {
            LOGGER.info("clean CloudServicesRequest: {}", k.getMetadata().getName());
            OperatorUtils.cloudServicesRequest(oc).delete(k);
        }
    }

    private void cleanKafkaConnection() {
        var c = OperatorUtils.kafkaConnection(oc).withName(KAFKA_CONNECTION_NAME).get();
        if (c != null) {
            LOGGER.info("clean ManagedKafkaConnection: {}", c.getMetadata().getName());
            OperatorUtils.kafkaConnection(oc).delete(c);
        }
    }

    private void collectOperatorLogs(ITestContext context) throws IOException {
        LogCollector.saveDeploymentLog(
            TestUtils.getLogPath(Environment.LOG_DIR.resolve("test-logs").toString(), context),
            oc,
            "openshift-operators",
            "service-binding-operator");

    }

    @AfterClass(alwaysRun = true)
    public void teardown(ITestContext context) {
        assumeTeardown();

        try {
            cleanKafkaConnection();
        } catch (Exception e) {
            LOGGER.error("clean kafka connection error: ", e);
        }

        try {
            cleanCloudServicesRequest();
        } catch (Exception e) {
            LOGGER.error("clean cloud services request error: ", e);
        }

        try {
            cleanCloudServiceAccountRequest();
        } catch (Exception e) {
            LOGGER.error("clean cloud service account request error: ", e);
        }

        try {
            cleanAccessTokenSecret();
        } catch (Exception e) {
            LOGGER.error("clean access token secret error: ", e);
        }

        try {
            collectOperatorLogs(context);
        } catch (Exception e) {
            LOGGER.error("collect operator logs error: ", e);
        }

        // force clean the service account if it hasn't done it yet
        try {
            SecurityMgmtAPIUtils.cleanServiceAccount(securityMgmtApi, SERVICE_ACCOUNT_NAME);
        } catch (Throwable t) {
            LOGGER.error("cleanServiceAccount error: ", t);
        }

        try {
            KafkaMgmtApiUtils.cleanKafkaInstance(kafkaMgmtApi, KAFKA_INSTANCE_NAME);
        } catch (Throwable t) {
            LOGGER.error("cleanKafkaInstance error: ", t);
        }
    }

    @Test
    public void testCreateAccessTokenSecret() {

        // Create Secret
        var data = new HashMap<String, String>();
        data.put("value", Base64.getEncoder().encodeToString(Environment.PRIMARY_OFFLINE_TOKEN.getBytes()));

        LOGGER.info("create access token secret with name: {}", ACCESS_TOKEN_SECRET_NAME);
        oc.secrets().create(OperatorUtils.buildSecret(ACCESS_TOKEN_SECRET_NAME, data));
    }

    @Test(dependsOnMethods = "testCreateAccessTokenSecret")
    public void testCreateCloudServiceAccountRequest() throws Throwable {

        var a = new CloudServiceAccountRequest();
        a.getMetadata().setName(CLOUD_SERVICE_ACCOUNT_REQUEST_NAME);
        a.setSpec(new CloudServiceAccountRequestSpec());
        a.getSpec().setServiceAccountName(SERVICE_ACCOUNT_NAME);
        a.getSpec().setServiceAccountDescription("");
        a.getSpec().setServiceAccountSecretName(SERVICE_ACCOUNT_SECRET_NAME);
        a.getSpec().setAccessTokenSecretName(ACCESS_TOKEN_SECRET_NAME);

        LOGGER.info("create CloudServiceAccountRequest with name: {}", CLOUD_SERVICE_ACCOUNT_REQUEST_NAME);
        a = OperatorUtils.cloudServiceAccountRequest(oc).create(a);
        LOGGER.info("created CloudServiceAccountRequest: {}", Json.encode(a));

        ReadyFunction<Void> ready = (__, ___) -> {
            var r = OperatorUtils.cloudServiceAccountRequest(oc).withName(CLOUD_SERVICE_ACCOUNT_REQUEST_NAME).get();
            LOGGER.debug(r);

            return r.getStatus() != null && r.getStatus().getMessage().equals("Created");
        };
        waitFor("CloudServiceAccountRequest to be created", ofSeconds(10), ofMinutes(4), ready);
        LOGGER.info("CloudServiceAccountRequest is created");
    }

    @Test(dependsOnMethods = "testCreateAccessTokenSecret")
    public void testCreateCloudServicesRequest() throws Throwable {

        var k = new CloudServicesRequest();
        k.getMetadata().setName(CLOUD_SERVICES_REQUEST_NAME);
        k.setSpec(new CloudServicesRequestSpec());
        k.getSpec().setAccessTokenSecretName(ACCESS_TOKEN_SECRET_NAME);

        LOGGER.info("create CloudServicesRequest with name: {}", CLOUD_SERVICES_REQUEST_NAME);
        k = OperatorUtils.cloudServicesRequest(oc).create(k);
        LOGGER.info("created CloudServicesRequest: {}", Json.encode(k));

        ReadyFunction<CloudServicesRequest> ready = (__, atom) -> {
            var r = OperatorUtils.cloudServicesRequest(oc).withName(CLOUD_SERVICES_REQUEST_NAME).get();
            LOGGER.debug(r);

            if (r.getStatus() != null
                && r.getStatus().getUserKafkas() != null
                && !r.getStatus().getUserKafkas().isEmpty()) {

                atom.set(r);
                return true;
            }
            return false;
        };
        cloudServicesRequest = waitFor("CloudServicesRequest to complete", ofSeconds(10), ofMinutes(3), ready);
        LOGGER.info("CloudServicesRequest is completed");
    }

    @Test(dependsOnMethods = {"testCreateCloudServiceAccountRequest", "testCreateCloudServicesRequest"})
    public void testCreateManagedKafkaConnection() throws Throwable {

        var userKafka = cloudServicesRequest.getStatus().getUserKafkas().stream()
            .filter(k -> k.getName().equals(KAFKA_INSTANCE_NAME))
            .findFirst();

        if (userKafka.isEmpty()) {
            LOGGER.info("CloudServicesRequest: {}", Json.encode(cloudServicesRequest));
            throw new TestException(message("failed to find the user kafka instance {} in the CloudServicesRequest {}", KAFKA_INSTANCE_NAME, CLOUD_SERVICES_REQUEST_NAME));
        }

        var c = new KafkaConnectionBuilder()
            .withNewMetadata()
                    .withName(KAFKA_CONNECTION_NAME)
            .endMetadata()
            .withNewSpec()
                .withAccessTokenSecretName(ACCESS_TOKEN_SECRET_NAME)
                .withKafkaId(userKafka.orElseThrow().getId())
                .withNewCredentials()
                    .withServiceAccountSecretName(SERVICE_ACCOUNT_SECRET_NAME)
                .endCredentials()
            .endSpec()
            .build();

        LOGGER.info("create ManagedKafkaConnection with name: {}", KAFKA_CONNECTION_NAME);
        c = OperatorUtils.kafkaConnection(oc).create(c);
        LOGGER.info("created ManagedKafkaConnection: {}", Json.encode(c));

        ReadyFunction<Void> ready = (__, ___) -> {
            var r = OperatorUtils.kafkaConnection(oc).withName(KAFKA_CONNECTION_NAME).get();
            LOGGER.debug(r);

            return r.getStatus() != null
                && r.getStatus().getMessage() != null
                && r.getStatus().getMessage().equals("Created");
        };
        waitFor("ManagedKafkaConnection to be created", ofSeconds(10), ofMinutes(2), ready);
        LOGGER.info("ManagedKafkaConnection is created");
    }
}
