package io.managed.services.test.kafka;


import com.openshift.cloud.api.kas.models.KafkaRequest;
import com.openshift.cloud.api.kas.models.KafkaRequestPayload;
import io.managed.services.test.Environment;
import io.managed.services.test.TestBase;
import io.managed.services.test.client.accountmgmt.AccountMgmtApi;
import io.managed.services.test.client.exception.ApiForbiddenException;
import io.managed.services.test.client.exception.ApiGenericException;
import io.managed.services.test.client.kafkamgmt.KafkaMgmtApi;
import io.managed.services.test.client.kafkamgmt.KafkaMgmtApiUtils;
import lombok.SneakyThrows;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.json.JSONObject;

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

/**
 * Test Quota for Kafka Instances to validate the types of Kafka Instances created are defined by the user quota.
 * There are 2 user for these test: 1 without quota (alien) and 1 with 1SKU quota (diff-org)
 * <p>
 * Tested operations:
 * <ul>
 *     <li> User with quota failed to create developer Kafka Instance
 *     <li> User with quota succeeded to create Standard Kafka Instance
 *     <li> User with quota failed to create Standard Kafka Instance when quota is reached
 *     <li> User with no quota failed to create Standard Kafka Instance
 *     <li> User with no quota succeeded to create developer Kafka Instance
 * </ul>
 * <p>
 * <b>Requires:</b>
 * <ul>
 *     <li> DIFF_ORG_OFFLINE_TOKEN
 *     <li> ALIEN_OFFLINE_TOKEN
 * </ul>
 */
public class QuotaKafkaInstanceTest extends TestBase {
    private static final Logger LOGGER = LogManager.getLogger(QuotaKafkaInstanceTest.class);

    private static final String KAFKA_INSTANCE_NAME_QUOTA = "mk-e2e-quota-" + Environment.LAUNCH_SUFFIX;
    private static final String KAFKA_INSTANCE_NAME_NO_QUOTA = "mk-e2e-no-quota-" + Environment.LAUNCH_SUFFIX;
    private static final String KAFKA_INSTANCE_NAME_FAIL = "mk-e2e-quota-fail-" + Environment.LAUNCH_SUFFIX;

    // Kafka instance plans
    private static final String PLAN_DEVELOPER = "developer.x1";
    private static final String PLAN_STANDARD = "standard.x1";
    
    // Kafka API code errors
    private static final String KAFKAS_MGMT_120_CODE = "KAFKAS-MGMT-120";
    private static final String KAFKAS_MGMT_120_REASON = "error getting billing model";
    private static final String KAFKAS_MGMT_21_CODE = "KAFKAS-MGMT-21";
    private static final String KAFKAS_MGMT_21_REASON = "unable to detect instance type in plan provided: ";

    private KafkaMgmtApi quotaUserKafkaMgmtApi;
    private KafkaMgmtApi noQuotaUserKafkaMgmtApi;

    @BeforeClass
    @SneakyThrows
    public void bootstrap() {
        assertNotNull(Environment.DIFF_ORG_OFFLINE_TOKEN, "the DIFF_ORG_OFFLINE_TOKEN env is null");
        assertNotNull(Environment.ALIEN_OFFLINE_TOKEN, "the ALIEN_OFFLINE_TOKEN env is null");

        quotaUserKafkaMgmtApi = KafkaMgmtApiUtils.kafkaMgmtApi(Environment.OPENSHIFT_API_URI, Environment.DIFF_ORG_OFFLINE_TOKEN);
        noQuotaUserKafkaMgmtApi = KafkaMgmtApiUtils.kafkaMgmtApi(Environment.OPENSHIFT_API_URI, Environment.ALIEN_OFFLINE_TOKEN);





        LOGGER.info("Preparing environment by deleting existing Kafka instances");
        deleteAllKafkaInstances();  
    }

    @AfterMethod(alwaysRun = true)
    public void teardown() {
        LOGGER.info("Cleaning up environment by deleting existing Kafka instances");
        deleteAllKafkaInstances();  
    }
    
    private void deleteAllKafkaInstances() {
        try {
            String noQuotaUserUsername = new AccountMgmtApi(Environment.OPENSHIFT_API_URI, Environment.ALIEN_OFFLINE_TOKEN).getAccountUsername();
            LOGGER.debug("try to delete all instances owned by user with name '{}'", noQuotaUserUsername);
            KafkaMgmtApiUtils.cleanKafkaInstanceByOwner(noQuotaUserKafkaMgmtApi, noQuotaUserUsername);
        } catch (Throwable t) {
            LOGGER.error("failed to clean kafka instance for ALIEN user: ", t);
        }

        try {
            String quotaUserUsername = new AccountMgmtApi(Environment.OPENSHIFT_API_URI, Environment.DIFF_ORG_OFFLINE_TOKEN).getAccountUsername();
            LOGGER.debug("try to delete all instances owned by user with name '{}'", quotaUserUsername);
            KafkaMgmtApiUtils.cleanKafkaInstanceByOwner(quotaUserKafkaMgmtApi, quotaUserUsername);
        } catch (Throwable t) {
            LOGGER.error("failed to clean kafka instances for DIFF_ORG user: ", t);
        }
    }

    @Test
    @SneakyThrows
    public void testQuotaUserFailedCreateDeveloperInstance() {
        LOGGER.info("Trying to create Developer Kafka instance '{}'", KAFKA_INSTANCE_NAME_FAIL);

        try {
            var payload = new KafkaRequestPayload()
                .name(KAFKA_INSTANCE_NAME_FAIL)
                .cloudProvider(Environment.CLOUD_PROVIDER)
                .plan(PLAN_DEVELOPER);
            KafkaMgmtApiUtils.createKafkaInstance(quotaUserKafkaMgmtApi, payload);
            fail("Kafka instance creation did NOT fail");
        } catch (ApiGenericException e) {
            assertEquals(e.getCode(), 400, "HTTP Status Response");
            JSONObject jsonResponse = new JSONObject(e.getResponseBody());  
            assertEquals(jsonResponse.get("code"), KAFKAS_MGMT_21_CODE);
            assertEquals(jsonResponse.get("reason"), KAFKAS_MGMT_21_REASON + "'" + PLAN_DEVELOPER + "'");
        }
    }

    @Test
    @SneakyThrows
    public void testQuotaUserSucceededCreateStandardInstance() {
        LOGGER.info("Trying to create Standard Kafka instance '{}'", KAFKA_INSTANCE_NAME_QUOTA);

        var payload = new KafkaRequestPayload()
            .name(KAFKA_INSTANCE_NAME_QUOTA)
            .cloudProvider(Environment.CLOUD_PROVIDER)
            .plan(PLAN_STANDARD);
        KafkaRequest kafka = KafkaMgmtApiUtils.createKafkaInstance(quotaUserKafkaMgmtApi, payload);
        assertEquals(kafka.getName(), KAFKA_INSTANCE_NAME_QUOTA);
        assertEquals(kafka.getInstanceType() + '.' + kafka.getSizeId(), PLAN_STANDARD);
    }

    @Test
    @SneakyThrows
    public void testQuotaUserFailedCreateStandardInstanceWhenQuotaIsReached() {
        LOGGER.info("Trying to create Standard Kafka instance when quota is reached '{}'", KAFKA_INSTANCE_NAME_FAIL);
        
        testQuotaUserSucceededCreateStandardInstance();
        try {
            var payload = new KafkaRequestPayload()
                .name(KAFKA_INSTANCE_NAME_FAIL)
                .cloudProvider(Environment.CLOUD_PROVIDER)
                .plan(PLAN_STANDARD);
            KafkaMgmtApiUtils.createKafkaInstance(quotaUserKafkaMgmtApi, payload);
            fail("Kafka instance creation did NOT fail");
        } catch (ApiForbiddenException e) {
            assertEquals(e.getCode(), 403, "HTTP Status Response");
            JSONObject jsonResponse = new JSONObject(e.getResponseBody());  
            assertEquals(jsonResponse.get("code"), KAFKAS_MGMT_120_CODE);
            assertEquals(jsonResponse.get("reason"), KAFKAS_MGMT_120_REASON);
        }
    }

    @Test
    @SneakyThrows
    public void testNoQuotaUserFailedCreateStandardInstance() {
        LOGGER.info("Trying to create Standard Kafka instance with no quota '{}'", KAFKA_INSTANCE_NAME_FAIL);

        try {
            var payload = new KafkaRequestPayload()
                .name(KAFKA_INSTANCE_NAME_FAIL)
                .cloudProvider(Environment.CLOUD_PROVIDER)
                .plan(PLAN_STANDARD);
            KafkaMgmtApiUtils.createKafkaInstance(noQuotaUserKafkaMgmtApi, payload);
            fail("Kafka instance creation did NOT fail");
        } catch (ApiGenericException e) {
            assertEquals(e.getCode(), 400, "HTTP Status Response");
            JSONObject jsonResponse = new JSONObject(e.getResponseBody());  
            assertEquals(jsonResponse.get("code"), KAFKAS_MGMT_21_CODE);
            assertEquals(jsonResponse.get("reason"), KAFKAS_MGMT_21_REASON + "'" + PLAN_STANDARD + "'");
        }
    }

    @Test
    @SneakyThrows
    public void testNoQuotaUserSucceededCreateDeveloperInstance() {
        LOGGER.info("Trying to create Developer Kafka instance with no quota '{}'", KAFKA_INSTANCE_NAME_NO_QUOTA);

        var payload = new KafkaRequestPayload()
            .name(KAFKA_INSTANCE_NAME_NO_QUOTA)
            .cloudProvider(Environment.CLOUD_PROVIDER)
            .plan(PLAN_DEVELOPER);
        KafkaRequest kafka = KafkaMgmtApiUtils.createKafkaInstance(noQuotaUserKafkaMgmtApi, payload);
        assertEquals(kafka.getName(), KAFKA_INSTANCE_NAME_NO_QUOTA);
        assertEquals(kafka.getInstanceType() + '.' + kafka.getSizeId(), PLAN_DEVELOPER);
    }
}
