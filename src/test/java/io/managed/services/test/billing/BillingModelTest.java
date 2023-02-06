package io.managed.services.test.billing;

import com.openshift.cloud.api.kas.models.KafkaRequest;
import com.openshift.cloud.api.kas.models.KafkaRequestPayload;
import io.managed.services.test.Environment;
import io.managed.services.test.client.exception.ApiGenericException;
import io.managed.services.test.client.kafkamgmt.KafkaMgmtApi;
import io.managed.services.test.client.kafkamgmt.KafkaMgmtApiUtils;
import io.managed.services.test.client.kafkamgmt.KafkaNotDeletedException;
import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j2;
import org.apache.http.HttpStatus;
import org.testng.Assert;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.managed.services.test.TestUtils.assumeTeardown;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


@Log4j2
public class BillingModelTest {

    static final String KAFKA_INSTANCE_NAME = "mk-e2e-"  + Environment.LAUNCH_SUFFIX;

    private KafkaMgmtApi kafkaMgmtApiStratosphere1;
    private KafkaMgmtApi kafkaMgmtApiStratosphere2;
    private KafkaMgmtApi kafkaMgmtApiStratosphere3;
    private KafkaMgmtApi kafkaMgmtApiStratosphere4;

    @BeforeClass
    public void bootstrap() {

        log.info("assert offline tokens of all used users");
        assertNotNull(Environment.STRATOSPHERE_SCENARIO_1_USER_OFFLINE_TOKEN, "the STRATOSPHERE_SCENARIO_1_USER_OFFLINE_TOKEN env is null");
        assertNotNull(Environment.STRATOSPHERE_SCENARIO_2_USER_OFFLINE_TOKEN, "the STRATOSPHERE_SCENARIO_2_USER_OFFLINE_TOKEN env is null");
        assertNotNull(Environment.STRATOSPHERE_SCENARIO_3_USER_OFFLINE_TOKEN, "the STRATOSPHERE_SCENARIO_3_USER_OFFLINE_TOKEN env is null");
        assertNotNull(Environment.STRATOSPHERE_SCENARIO_4_USER_OFFLINE_TOKEN, "the STRATOSPHERE_SCENARIO_4_USER_OFFLINE_TOKEN env is null");

        log.info("assert associated accounts of all used users");
        assertNotNull(Environment.STRATOSPHERE_SCENARIO_1_AWS_ACCOUNT_ID, "the STRATOSPHERE_SCENARIO_1_AWS_ACCOUNT_ID env is null");
        assertNotNull(Environment.STRATOSPHERE_SCENARIO_2_AWS_ACCOUNT_ID, "the STRATOSPHERE_SCENARIO_2_AWS_ACCOUNT_ID env is null");
        assertNotNull(Environment.STRATOSPHERE_SCENARIO_3_AWS_ACCOUNT_ID, "the STRATOSPHERE_SCENARIO_3_AWS_ACCOUNT_ID env is null");
        assertNotNull(Environment.STRATOSPHERE_SCENARIO_3_RHM_ACCOUNT_ID, "the STRATOSPHERE_SCENARIO_3_RHM_ACCOUNT_ID env is null");
        assertNotNull(Environment.STRATOSPHERE_SCENARIO_4_AWS_ACCOUNT_ID, "the STRATOSPHERE_SCENARIO_4_AWS_ACCOUNT_ID env is null");

        // setApis
        log.info("set up application services for all stratosphere users");
        kafkaMgmtApiStratosphere1 = KafkaMgmtApiUtils.kafkaMgmtApi(Environment.OPENSHIFT_API_URI, Environment.STRATOSPHERE_SCENARIO_1_USER_OFFLINE_TOKEN);
        kafkaMgmtApiStratosphere2 = KafkaMgmtApiUtils.kafkaMgmtApi(Environment.OPENSHIFT_API_URI, Environment.STRATOSPHERE_SCENARIO_2_USER_OFFLINE_TOKEN);
        kafkaMgmtApiStratosphere3 = KafkaMgmtApiUtils.kafkaMgmtApi(Environment.OPENSHIFT_API_URI, Environment.STRATOSPHERE_SCENARIO_3_USER_OFFLINE_TOKEN);
        kafkaMgmtApiStratosphere4 = KafkaMgmtApiUtils.kafkaMgmtApi(Environment.OPENSHIFT_API_URI, Environment.STRATOSPHERE_SCENARIO_4_USER_OFFLINE_TOKEN);

    }

    @AfterClass(alwaysRun = true)
    public void teardown() {
        assumeTeardown();
    }

    private void cleanup(KafkaMgmtApi kafkaMgmtApi) throws KafkaNotDeletedException, ApiGenericException, InterruptedException {
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
        if ("gcp".equals(Environment.CLOUD_PROVIDER)) {
            throw new SkipException("gcp marketplace is not available as a billing option at this time");
        }
//        String user = Environment.STRATOSPHERE_SCENARIO_1_USER;
        KafkaMgmtApi kafkaMgmtApi = kafkaMgmtApiStratosphere1;

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
            cleanup(kafkaMgmtApi);
        }
    }


    @Test
    @SneakyThrows
    // User sets the billing_model to standard.
    // Outcome: failure, the organization does not have standard quota.
    public void testFailWhenBillingModelIsNotAvailable() {
//        String user = Environment.STRATOSPHERE_SCENARIO_1_USER;
        KafkaMgmtApi kafkaMgmtApi =  kafkaMgmtApiStratosphere1;

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
            assertEquals(ex.getCode(), HttpStatus.SC_FORBIDDEN);

            var body = ex.decode();
            assertEquals(body.reason, "Insufficient quota: Insufficient Quota");
            assertEquals(body.id, ApiGenericException.API_ERROR_INSUFFICIENT_QUOTA);
        } finally {
            cleanup(kafkaMgmtApi);
        }
    }


    @Test
    @SneakyThrows
    // User sets the billing_cloud_account_id to a value that does not match the linked account.
    // Outcome: failure, no matching cloud account.
    public void testFailWhenCloudAccountNotFound() {
//        String user = Environment.STRATOSPHERE_SCENARIO_1_USER;
        KafkaMgmtApi kafkaMgmtApi = kafkaMgmtApiStratosphere1;

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
                    String.format("Billing account id missing or invalid: we have not been able to validate your billingAccountID",
                            Environment.STRATOSPHERE_SCENARIO_1_AWS_ACCOUNT_ID));
        } finally {
            cleanup(kafkaMgmtApi);
        }
    }

    @Test
    @SneakyThrows
    // User sets the marketplace to RHM.
    // Outcome: failure, no cloud account linked for that marketplace.
    public void testFailWhenMarketplaceNotAvailable() {
//        String user = Environment.STRATOSPHERE_SCENARIO_1_USER;
        KafkaMgmtApi kafkaMgmtApi = kafkaMgmtApiStratosphere1;

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
            cleanup(kafkaMgmtApi);
        }
    }

    @Test
    @SneakyThrows
    // User does not provide any of the parameters billing_cloud_account_id, marketplace, billing_model.
    // Outcome: success, standard quota is chosen.
    public void testDefaultToStandardWhenNoCloudAccountAvailable() {
        KafkaMgmtApi kafkaMgmtApi = kafkaMgmtApiStratosphere2;

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
            cleanup(kafkaMgmtApi);
        }
    }

    @Test
    @SneakyThrows
    // User sets the billing_cloud_account_id to a value that does match the linked account.
    // Outcome: success, marketplace cloud account is chosen.
    public void testSubmitValidAWSCloudAccount() {
        if ("gcp".equals(Environment.CLOUD_PROVIDER)) {
            throw new SkipException("gcp marketplace is not available as a billing option at this time");
        }
        KafkaMgmtApi kafkaMgmtApi = kafkaMgmtApiStratosphere2;

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
            cleanup(kafkaMgmtApi);
        }
    }

    @Test
    @SneakyThrows
    // The customer provides the billing_model set to the marketplace.
    // Outcome: success, marketplace refers to marketplace-rhm.
    public void testAutomaticallyPickMarketplaceRhmWhenBillingModelIsMarketplace() {
        KafkaMgmtApi kafkaMgmtApi = kafkaMgmtApiStratosphere3;

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
            assertNotNull(kafka);
            assertEquals(kafka.getBillingModel(), "marketplace");
        } finally {
            cleanup(kafkaMgmtApi);
        }
    }

    @Test
    @SneakyThrows
    // The customers provide the billing_cloud_account_id of the linked AWS account.
    // Outcome: success, AWS cloud account chosen because there is only one matching account.
    public void testSelectValidAWSCloudAccountFromMultiple() {
        if ("gcp".equals(Environment.CLOUD_PROVIDER)) {
            throw new SkipException("gcp marketplace is not available as a billing option at this time");
        }
        KafkaMgmtApi kafkaMgmtApi = kafkaMgmtApiStratosphere3;

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
            cleanup(kafkaMgmtApi);
        }
    }

    @Test
    @SneakyThrows
    // The customers provide the billing_cloud_account_id of the linked RHM account but with the marketplace set to AWS.
    // Outcome: failure, no matching cloud account found.
    public void testFailWhenMarketplaceDoesNotMatchCloudAccount() {
        KafkaMgmtApi kafkaMgmtApi = kafkaMgmtApiStratosphere3;

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
            assertTrue(body.reason.contains("Billing account id missing or invalid: we have not been able to validate your billingAccountID"));
            assertEquals(body.id, ApiGenericException.API_ERROR_BILLING_ACCOUNT_INVALID);
        } finally {
            cleanup(kafkaMgmtApi);
        }
    }


    @Test
    @SneakyThrows
    // The customers provide the billing_cloud_account_id of the linked RHM account.
    // Outcome: success, RHM cloud account chosen because there is only one matching account.
    public void testCreateWithValidRHMCloudAccount() {
        KafkaMgmtApi kafkaMgmtApi = kafkaMgmtApiStratosphere3;

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
            cleanup(kafkaMgmtApi);
        }
    }

    @Test
    @SneakyThrows
    // The customers provide the billing_model set to the marketplace and the marketplace set to AWS.
    // Outcome: failure, ambiguous cloud accounts.
    public void testFailWhenNoCloudAccountIsChosenAndMultipleAvailable() {
        KafkaMgmtApi kafkaMgmtApi = kafkaMgmtApiStratosphere4;

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
            cleanup(kafkaMgmtApi);
        }
    }

    @Test
    @SneakyThrows
    // The customer provides the billing_cloud_account_id of one of the AWS accounts.
    // Outcome: success, exactly one cloud account matches.
    public void testCreateWithValidAWSAccountFromMultiple() {
        if ("gcp".equals(Environment.CLOUD_PROVIDER)) {
            throw new SkipException("gcp marketplace is not available as a billing option at this time");
        }
        KafkaMgmtApi kafkaMgmtApi = kafkaMgmtApiStratosphere4;

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
            cleanup(kafkaMgmtApi);
        }
    }

    @Test
    @SneakyThrows
    public void testFailOnAwsAccountWithGcpProvider() {
        KafkaMgmtApi kafkaMgmtApi = kafkaMgmtApiStratosphere4;

        String cloudAccountId = Environment.STRATOSPHERE_SCENARIO_4_AWS_ACCOUNT_ID;
        var payload = new KafkaRequestPayload()
            .name(KAFKA_INSTANCE_NAME)
            .cloudProvider("gcp")
            .region("us-east1")
            .billingCloudAccountId(cloudAccountId);

        log.info("create kafka instance '{}'", payload.getName());

        Assert.assertThrows(ApiGenericException.class, () -> KafkaMgmtApiUtils.createKafkaInstance(kafkaMgmtApi, payload));
    }
}
