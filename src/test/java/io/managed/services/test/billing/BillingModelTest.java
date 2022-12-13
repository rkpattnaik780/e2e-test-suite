package io.managed.services.test.billing;

import com.openshift.cloud.api.kas.models.KafkaRequest;
import com.openshift.cloud.api.kas.models.KafkaRequestPayload;
import io.managed.services.test.Environment;
import io.managed.services.test.client.ApplicationServicesApi;
import io.managed.services.test.client.exception.ApiGenericException;
import io.managed.services.test.client.kafkamgmt.KafkaMgmtApi;
import io.managed.services.test.client.kafkamgmt.KafkaMgmtApiUtils;
import io.managed.services.test.client.kafkamgmt.KafkaNotDeletedException;
import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j2;
import org.apache.http.HttpStatus;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static io.managed.services.test.TestUtils.assumeTeardown;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


@Log4j2
public class BillingModelTest {

    static final String KAFKA_INSTANCE_NAME = "mk-e2e-" + Environment.LAUNCH_KEY;

    private final Map<String, KafkaMgmtApi> mgmtApis = new HashMap<>();

    @BeforeClass
    public void bootstrap() {
        assertNotNull(Environment.STRATOSPHERE_SCENARIO_1_USER, "the STRATOSPHERE_SCENARIO_1_USER env is null");
        assertNotNull(Environment.STRATOSPHERE_SCENARIO_2_USER, "the STRATOSPHERE_SCENARIO_2_USER env is null");
        assertNotNull(Environment.STRATOSPHERE_SCENARIO_3_USER, "the STRATOSPHERE_SCENARIO_3_USER env is null");
        assertNotNull(Environment.STRATOSPHERE_SCENARIO_4_USER, "the STRATOSPHERE_SCENARIO_4_USER env is null");
        assertNotNull(Environment.STRATOSPHERE_PASSWORD, "the STRATOSPHERE_PASSWORD env is null");
        assertNotNull(Environment.STRATOSPHERE_SCENARIO_1_AWS_ACCOUNT_ID, "the STRATOSPHERE_SCENARIO_1_AWS_ACCOUNT_ID env is null");
        assertNotNull(Environment.STRATOSPHERE_SCENARIO_2_AWS_ACCOUNT_ID, "the STRATOSPHERE_SCENARIO_2_AWS_ACCOUNT_ID env is null");
        assertNotNull(Environment.STRATOSPHERE_SCENARIO_3_AWS_ACCOUNT_ID, "the STRATOSPHERE_SCENARIO_3_AWS_ACCOUNT_ID env is null");
        assertNotNull(Environment.STRATOSPHERE_SCENARIO_3_RHM_ACCOUNT_ID, "the STRATOSPHERE_SCENARIO_3_RHM_ACCOUNT_ID env is null");
        assertNotNull(Environment.STRATOSPHERE_SCENARIO_4_AWS_ACCOUNT_ID, "the STRATOSPHERE_SCENARIO_4_AWS_ACCOUNT_ID env is null");
    }

    @AfterClass(alwaysRun = true)
    public void teardown() {
        assumeTeardown();
    }

    // gets and caches Kafka management APIs per user
    private KafkaMgmtApi getMgmtApiForUser(String user) {
        var api = mgmtApis.get(user);
        if (api != null) {
            return api;
        }

        var apps = ApplicationServicesApi.applicationServicesApi(
                user,
                Environment.STRATOSPHERE_PASSWORD);

        mgmtApis.put(user, apps.kafkaMgmt());
        return mgmtApis.get(user);
    }

    private void cleanup(String user) throws KafkaNotDeletedException, ApiGenericException, InterruptedException {
        KafkaMgmtApi kafkaMgmtApi = getMgmtApiForUser(user);
        Optional<KafkaRequest> kafka = KafkaMgmtApiUtils.getKafkaByName(kafkaMgmtApi, KAFKA_INSTANCE_NAME);
        if (kafka.isPresent()) {
            KafkaMgmtApiUtils.cleanKafkaInstance(kafkaMgmtApi, KAFKA_INSTANCE_NAME);
            KafkaMgmtApiUtils.waitUntilKafkaIsDeleted(kafkaMgmtApi, kafka.get().getId());
        }
    }

    @Test
    @SneakyThrows
    // User does not provide any of billing_model, billing_cloud_account_id, marketplace.
    // Outcome: success, existing single cloud account is automatically chosen
    public void testAutomaticallyPickSingleCloudAccount() {
        String user = Environment.STRATOSPHERE_SCENARIO_1_USER;
        KafkaMgmtApi kafkaMgmtApi = getMgmtApiForUser(user);

        var payload = new KafkaRequestPayload()
                .name(KAFKA_INSTANCE_NAME)
                .cloudProvider(Environment.CLOUD_PROVIDER)
                .region(Environment.DEFAULT_KAFKA_REGION);

        KafkaRequest kafka;
        log.info("create kafka instance '{}'", payload.getName());
        try {
            kafka = KafkaMgmtApiUtils.createKafkaInstance(kafkaMgmtApi, payload);
            log.debug(kafka);
            assertEquals(kafka.getBillingModel(), "marketplace");
            assertEquals(kafka.getMarketplace(), "aws");
            assertEquals(kafka.getBillingCloudAccountId(), Environment.STRATOSPHERE_SCENARIO_1_AWS_ACCOUNT_ID);
        } finally {
            cleanup(user);
        }
    }


    @Test
    @SneakyThrows
    // User sets the billing_model to standard.
    // Outcome: failure, the organization does not have standard quota.
    public void testFailWhenBillingModelIsNotAvailable() {
        String user = Environment.STRATOSPHERE_SCENARIO_1_USER;
        KafkaMgmtApi kafkaMgmtApi = getMgmtApiForUser(user);

        var payload = new KafkaRequestPayload()
                .name(KAFKA_INSTANCE_NAME)
                .cloudProvider(Environment.CLOUD_PROVIDER)
                .region(Environment.DEFAULT_KAFKA_REGION)
                .billingModel("standard");

        log.info("create kafka instance '{}'", payload.getName());
        KafkaRequest kafka;
        try {
            kafka = KafkaMgmtApiUtils.createKafkaInstance(kafkaMgmtApi, payload);

            // if we reach this line, the test has failed
            log.debug(kafka);
            assertNull(kafka);
        } catch (ApiGenericException ex) {
            assertEquals(ex.getCode(), HttpStatus.SC_BAD_REQUEST);

            var body = ex.decode();
            assertEquals(body.reason, "Billing account id missing or invalid: requested billing model does not match assigned. requested: standard, assigned: marketplace-aws");
            assertEquals(body.id, ApiGenericException.API_ERROR_BILLING_ACCOUNT_INVALID);
        } finally {
            cleanup(user);
        }
    }


    @Test
    @SneakyThrows
    // User sets the billing_cloud_account_id to a value that does not match the linked account.
    // Outcome: failure, no matching cloud account.
    public void testFailWhenCloudAccountNotFound() {
        String user = Environment.STRATOSPHERE_SCENARIO_1_USER;
        KafkaMgmtApi kafkaMgmtApi = getMgmtApiForUser(user);

        var payload = new KafkaRequestPayload()
                .name(KAFKA_INSTANCE_NAME)
                .cloudProvider(Environment.CLOUD_PROVIDER)
                .region(Environment.DEFAULT_KAFKA_REGION)
                .billingModel("marketplace")
                .billingCloudAccountId("dummy");

        log.info("create kafka instance '{}'", payload.getName());
        KafkaRequest kafka;
        try {
            kafka = KafkaMgmtApiUtils.createKafkaInstance(kafkaMgmtApi, payload);

            // if we reach this line, the test has failed
            log.debug(kafka);
            assertNull(kafka);
        } catch (ApiGenericException ex) {
            assertEquals(ex.getCode(), HttpStatus.SC_BAD_REQUEST);
            var body = ex.decode();
            assertEquals(body.id, ApiGenericException.API_ERROR_BILLING_ACCOUNT_INVALID);
            assertEquals(body.reason,
                    String.format("Billing account id missing or invalid: no matching billing account found. " +
                                    "Provided: dummy, Available: [{3 %s aws}]",
                            Environment.STRATOSPHERE_SCENARIO_1_AWS_ACCOUNT_ID));
        } finally {
            cleanup(user);
        }
    }

    @Test
    @SneakyThrows
    // User sets the marketplace to RHM.
    // Outcome: failure, no cloud account linked for that marketplace.
    public void testFailWhenMarketplaceNotAvailable() {
        String user = Environment.STRATOSPHERE_SCENARIO_1_USER;
        KafkaMgmtApi kafkaMgmtApi = getMgmtApiForUser(user);

        var payload = new KafkaRequestPayload()
                .name(KAFKA_INSTANCE_NAME)
                .cloudProvider(Environment.CLOUD_PROVIDER)
                .region(Environment.DEFAULT_KAFKA_REGION)
                .billingModel("marketplace")
                .marketplace("rhm");

        log.info("create kafka instance '{}'", payload.getName());
        KafkaRequest kafka;
        try {
            kafka = KafkaMgmtApiUtils.createKafkaInstance(kafkaMgmtApi, payload);

            // if we reach this line, the test has failed
            log.debug(kafka);
            assertNull(kafka);
        } catch (ApiGenericException ex) {
            assertEquals(ex.getCode(), HttpStatus.SC_BAD_REQUEST);
            var body = ex.decode();
            assertEquals(body.id, ApiGenericException.API_ERROR_BILLING_ACCOUNT_INVALID);
            assertEquals(body.reason, "Billing account id missing or invalid: no billing account provided for marketplace: rhm");
        } finally {
            cleanup(user);
        }
    }

    @Test
    @SneakyThrows
    // User does not provide any of the parameters billing_cloud_account_id, marketplace, billing_model.
    // Outcome: success, standard quota is chosen.
    public void testDefaultToStandardWhenNoCloudAccountAvailable() {
        String user = Environment.STRATOSPHERE_SCENARIO_2_USER;
        KafkaMgmtApi kafkaMgmtApi = getMgmtApiForUser(user);

        var payload = new KafkaRequestPayload()
                .name(KAFKA_INSTANCE_NAME)
                .cloudProvider(Environment.CLOUD_PROVIDER)
                .region(Environment.DEFAULT_KAFKA_REGION);

        log.info("create kafka instance '{}'", payload.getName());
        KafkaRequest kafka;
        try {
            kafka = KafkaMgmtApiUtils.createKafkaInstance(kafkaMgmtApi, payload);
            log.debug(kafka);
            assertNotNull(kafka);
            assertNull(kafka.getMarketplace());
            assertNull(kafka.getBillingCloudAccountId());
            assertEquals(kafka.getBillingModel(), "standard");
        } finally {
            cleanup(user);
        }
    }

    @Test
    @SneakyThrows
    // User sets the billing_cloud_account_id to a value that does match the linked account.
    // Outcome: success, marketplace cloud account is chosen.
    public void testSubmitValidAWSCloudAccount() {
        String user = Environment.STRATOSPHERE_SCENARIO_2_USER;
        KafkaMgmtApi kafkaMgmtApi = getMgmtApiForUser(user);

        String cloudAccountId = Environment.STRATOSPHERE_SCENARIO_2_AWS_ACCOUNT_ID;
        var payload = new KafkaRequestPayload()
                .name(KAFKA_INSTANCE_NAME)
                .cloudProvider(Environment.CLOUD_PROVIDER)
                .region(Environment.DEFAULT_KAFKA_REGION)
                .billingCloudAccountId(cloudAccountId);

        log.info("create kafka instance '{}'", payload.getName());
        KafkaRequest kafka;
        try {
            kafka = KafkaMgmtApiUtils.createKafkaInstance(kafkaMgmtApi, payload);
            log.debug(kafka);
            assertNotNull(kafka);
            assertEquals(kafka.getMarketplace(), "aws");
            assertEquals(kafka.getBillingCloudAccountId(), cloudAccountId);
            assertEquals(kafka.getBillingModel(), "marketplace");
        } finally {
            cleanup(user);
        }
    }

    @Test
    @SneakyThrows
    // The customer provides the billing_model set to the marketplace.
    // Outcome: failure, ambiguous cloud accounts.
    public void testFailWhenCloudAccountsAreAmbiguous() {
        String user = Environment.STRATOSPHERE_SCENARIO_3_USER;
        KafkaMgmtApi kafkaMgmtApi = getMgmtApiForUser(user);

        var payload = new KafkaRequestPayload()
                .name(KAFKA_INSTANCE_NAME)
                .cloudProvider(Environment.CLOUD_PROVIDER)
                .region(Environment.DEFAULT_KAFKA_REGION)
                .billingModel("marketplace");

        log.info("create kafka instance '{}'", payload.getName());
        KafkaRequest kafka;
        try {
            kafka = KafkaMgmtApiUtils.createKafkaInstance(kafkaMgmtApi, payload);
            log.debug(kafka);

            // if we reach this line, the test has failed
            assertNull(kafka);
        } catch (ApiGenericException ex) {
            assertEquals(ex.getCode(), HttpStatus.SC_BAD_REQUEST);
            var body = ex.decode();
            assertEquals(body.id, ApiGenericException.API_ERROR_BILLING_ACCOUNT_INVALID);
        } finally {
            cleanup(user);
        }
    }

    @Test
    @SneakyThrows
    // The customers provide the billing_cloud_account_id of the linked AWS account.
    // Outcome: success, AWS cloud account chosen because there is only one matching account.
    public void testSelectValidAWSCloudAccountFromMultiple() {
        String user = Environment.STRATOSPHERE_SCENARIO_3_USER;
        KafkaMgmtApi kafkaMgmtApi = getMgmtApiForUser(user);

        String cloudAccountId = Environment.STRATOSPHERE_SCENARIO_3_AWS_ACCOUNT_ID;
        var payload = new KafkaRequestPayload()
                .name(KAFKA_INSTANCE_NAME)
                .cloudProvider(Environment.CLOUD_PROVIDER)
                .region(Environment.DEFAULT_KAFKA_REGION)
                .billingCloudAccountId(cloudAccountId);

        log.info("create kafka instance '{}'", payload.getName());
        KafkaRequest kafka;
        try {
            kafka = KafkaMgmtApiUtils.createKafkaInstance(kafkaMgmtApi, payload);
            assertNotNull(kafka);
            log.debug(kafka);
            assertEquals(kafka.getMarketplace(), "aws");
            assertEquals(kafka.getBillingCloudAccountId(), cloudAccountId);
            assertEquals(kafka.getBillingModel(), "marketplace");
        } finally {
            cleanup(user);
        }
    }

    @Test
    @SneakyThrows
    // The customers provide the billing_cloud_account_id of the linked RHM account but with the marketplace set to AWS.
    // Outcome: failure, no matching cloud account found.
    public void testFailWhenMarketplaceDoesNotMatchCloudAccount() {
        String user = Environment.STRATOSPHERE_SCENARIO_3_USER;
        KafkaMgmtApi kafkaMgmtApi = getMgmtApiForUser(user);

        String cloudAccountId = Environment.STRATOSPHERE_SCENARIO_3_RHM_ACCOUNT_ID;
        var payload = new KafkaRequestPayload()
                .name(KAFKA_INSTANCE_NAME)
                .cloudProvider(Environment.CLOUD_PROVIDER)
                .region(Environment.DEFAULT_KAFKA_REGION)
                .billingCloudAccountId(cloudAccountId)
                .marketplace("aws");

        log.info("create kafka instance '{}'", payload.getName());
        KafkaRequest kafka;
        try {
            kafka = KafkaMgmtApiUtils.createKafkaInstance(kafkaMgmtApi, payload);

            // if we reach this line, the test has failed
            log.debug(kafka);
            assertNull(kafka);
        } catch (ApiGenericException ex) {
            assertEquals(ex.getCode(), HttpStatus.SC_BAD_REQUEST);
            var body = ex.decode();
            // TODO the error message in the fleet manager needs to be improved here to include the marketplace
            assertTrue(body.reason.contains("Billing account id missing or invalid: no matching billing account found."));
            assertTrue(body.reason.contains(Environment.STRATOSPHERE_SCENARIO_3_RHM_ACCOUNT_ID));
            assertTrue(body.reason.contains(Environment.STRATOSPHERE_SCENARIO_3_AWS_ACCOUNT_ID));
            assertEquals(body.id, ApiGenericException.API_ERROR_BILLING_ACCOUNT_INVALID);
        } finally {
            cleanup(user);
        }
    }


    @Test
    @SneakyThrows
    // The customers provide the billing_cloud_account_id of the linked RHM account.
    // Outcome: success, RHM cloud account chosen because there is only one matching account.
    public void testCreateWithValidRHMCloudAccount() {
        String user = Environment.STRATOSPHERE_SCENARIO_3_USER;
        KafkaMgmtApi kafkaMgmtApi = getMgmtApiForUser(user);

        String cloudAccountId = Environment.STRATOSPHERE_SCENARIO_3_RHM_ACCOUNT_ID;
        var payload = new KafkaRequestPayload()
                .name(KAFKA_INSTANCE_NAME)
                .cloudProvider(Environment.CLOUD_PROVIDER)
                .region(Environment.DEFAULT_KAFKA_REGION)
                .billingCloudAccountId(cloudAccountId);

        log.info("create kafka instance '{}'", payload.getName());
        KafkaRequest kafka;
        try {
            kafka = KafkaMgmtApiUtils.createKafkaInstance(kafkaMgmtApi, payload);
            log.debug(kafka);
            assertNotNull(kafka);
            assertEquals(kafka.getMarketplace(), "rhm");
            assertEquals(kafka.getBillingCloudAccountId(), cloudAccountId);
            assertEquals(kafka.getBillingModel(), "marketplace");
        } finally {
            cleanup(user);
        }
    }

    @Test
    @SneakyThrows
    // The customers provide the billing_model set to the marketplace and the marketplace set to AWS.
    // Outcome: failure, ambiguous cloud accounts.
    public void testFailWhenNoCloudAccountIsChosenAndMultipleAvailable() {
        String user = Environment.STRATOSPHERE_SCENARIO_4_USER;
        KafkaMgmtApi kafkaMgmtApi = getMgmtApiForUser(user);

        var payload = new KafkaRequestPayload()
                .name(KAFKA_INSTANCE_NAME)
                .cloudProvider(Environment.CLOUD_PROVIDER)
                .region(Environment.DEFAULT_KAFKA_REGION)
                .marketplace("aws")
                .billingModel("marketplace");

        log.info("create kafka instance '{}'", payload.getName());
        KafkaRequest kafka;
        try {
            kafka = KafkaMgmtApiUtils.createKafkaInstance(kafkaMgmtApi, payload);

            // if we reach this line, the test has failed
            log.debug(kafka);
            assertNull(kafka);
        } catch (ApiGenericException ex) {
            assertEquals(ex.getCode(), HttpStatus.SC_BAD_REQUEST);
            var body = ex.decode();
            assertEquals(body.reason, "Billing account id missing or invalid: no billing account provided for marketplace: aws");
            assertEquals(body.id, ApiGenericException.API_ERROR_BILLING_ACCOUNT_INVALID);
        } finally {
            cleanup(user);
        }
    }

    @Test
    @SneakyThrows
    // The customer provides the billing_cloud_account_id of one of the AWS accounts.
    // Outcome: success, exactly one cloud account matches.
    public void testCreateWithValidAWSAccountFromMultiple() {
        String user = Environment.STRATOSPHERE_SCENARIO_4_USER;
        KafkaMgmtApi kafkaMgmtApi = getMgmtApiForUser(user);

        String cloudAccountId = Environment.STRATOSPHERE_SCENARIO_4_AWS_ACCOUNT_ID;
        var payload = new KafkaRequestPayload()
                .name(KAFKA_INSTANCE_NAME)
                .cloudProvider(Environment.CLOUD_PROVIDER)
                .region(Environment.DEFAULT_KAFKA_REGION)
                .billingCloudAccountId(cloudAccountId);

        log.info("create kafka instance '{}'", payload.getName());

        try {
            KafkaRequest kafka = KafkaMgmtApiUtils.createKafkaInstance(kafkaMgmtApi, payload);
            log.debug(kafka);
            assertNotNull(kafka);
            assertEquals(kafka.getMarketplace(), "aws");
            assertEquals(kafka.getBillingCloudAccountId(), cloudAccountId);
            assertEquals(kafka.getBillingModel(), "marketplace");
        } finally {
            cleanup(user);
        }
    }
}
