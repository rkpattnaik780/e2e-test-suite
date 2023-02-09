package io.managed.services.test.rhoas;

import com.openshift.cloud.api.kas.auth.models.AclBinding;
import com.openshift.cloud.api.kas.auth.models.AclBindingListPage;
import com.openshift.cloud.api.kas.auth.models.AclOperation;
import com.openshift.cloud.api.kas.auth.models.AclPermissionType;
import com.openshift.cloud.api.kas.auth.models.AclResourceType;
import com.openshift.cloud.api.kas.models.KafkaRequest;
import com.openshift.cloud.api.serviceaccounts.models.ServiceAccountData;
import io.managed.services.test.Environment;
import io.managed.services.test.TestBase;
import io.managed.services.test.cli.CLI;
import io.managed.services.test.cli.CLIDownloader;
import io.managed.services.test.cli.CLIUtils;
import io.managed.services.test.cli.CliGenericException;
import io.managed.services.test.cli.ServiceAccountSecret;
import io.managed.services.test.client.kafkamgmt.KafkaMgmtApiUtils;
import io.managed.services.test.client.securitymgmt.SecurityMgmtAPIUtils;
import io.vertx.core.Vertx;
import lombok.SneakyThrows;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.util.Optional;

import static io.managed.services.test.TestUtils.bwait;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


/**
 * Test the application services CLI[1] kafka commands.
 * <p>
 * The tests download the CLI from GitHub to the local machine where the test suite is running
 * and perform all operations using the CLI.
 * <p>
 * By default the latest version of the CLI is downloaded otherwise a specific version can be set using
 * the CLI_VERSION env. The CLI platform (linux, mac, win) and arch (amd64, arm) is automatically detected,
 * or it can be enforced using the CLI_PLATFORM and CLI_ARCH env.
 * <p>
 * 1. https://github.com/redhat-developer/app-services-cli
 * <p>
 * <b>Requires:</b>
 * <ul>
 *     <li> PRIMARY_OFFLINE_TOKEN
 * </ul>
 */
@Test
public class KafkaRhoasAclTests extends TestBase {
    private static final Logger LOGGER = LogManager.getLogger(KafkaRhoasAclTests.class);

    private static final String KAFKA_INSTANCE_NAME = "cli-e2e-test-" + Environment.LAUNCH_SUFFIX;
    private static final String SERVICE_ACCOUNT_NAME = "cli-e2e-service-account-"  + Environment.LAUNCH_SUFFIX;
    private static final String TOPIC_NAME = "cli-e2e-test-topic";
    private static final String DIFF_USER = "external-user";

    private final Vertx vertx = Vertx.vertx();

    private CLI cli;

    private KafkaRequest kafka;
    private ServiceAccountSecret serviceAccountSecret;
    private ServiceAccountData serviceAccount;

    @BeforeClass
    @SneakyThrows
    public void bootstrap() {
        assertNotNull(Environment.PRIMARY_OFFLINE_TOKEN, "the PRIMARY_OFFLINE_TOKEN env is null");

        downloadCLI();
        login();
        createServiceAccount();
        createKafkaInstance();
    }

    @AfterClass(alwaysRun = true)
    @SneakyThrows
    public void clean() {

        var offlineToken = Environment.PRIMARY_OFFLINE_TOKEN;

        var kafkaMgmtApi =  KafkaMgmtApiUtils.kafkaMgmtApi(Environment.OPENSHIFT_API_URI, offlineToken);
        var securityMgmtApi = SecurityMgmtAPIUtils.securityMgmtApi(Environment.OPENSHIFT_API_URI, offlineToken);

        try {
            cli.deleteAllAcls(DIFF_USER);
        } catch (Throwable t) {
            LOGGER.error("delete ACLs error: ", t);
        }

        try {
            cli.deleteAllAcls(serviceAccount);
        } catch (Throwable t) {
            LOGGER.error("delete ACLs error: ", t);
        }

        try {
            KafkaMgmtApiUtils.deleteKafkaByNameIfExists(kafkaMgmtApi, KAFKA_INSTANCE_NAME);
        } catch (Throwable t) {
            LOGGER.error("delete kafka instance error: ", t);
        }

        try {
            SecurityMgmtAPIUtils.cleanServiceAccount(securityMgmtApi, SERVICE_ACCOUNT_NAME);
        } catch (Throwable t) {
            LOGGER.error("delete service account error: ", t);
        }

        try {
            LOGGER.info("logout user from rhoas");
            cli.logout();
        } catch (Throwable t) {
            LOGGER.error("CLI logout error: ", t);
        }

        try {
            LOGGER.info("delete workdir: {}", cli.getWorkdir());
            FileUtils.deleteDirectory(new File(cli.getWorkdir()));
        } catch (Throwable t) {
            LOGGER.error("clean workdir error: ", t);
        }

        bwait(vertx.close());
    }

    @SneakyThrows
    public void downloadCLI() {

        var downloader = CLIDownloader.defaultDownloader();

        // download the cli
        var binary = downloader.downloadCLIInTempDir();

        this.cli = new CLI(binary);

        LOGGER.info("validate cli");
        LOGGER.debug(cli.help());
    }

    @SneakyThrows
    public void login() {

        // make sure you are logout while developing locally.
        LOGGER.info("verify that we aren't logged-in");
        assertThrows(CliGenericException.class, () -> cli.listKafka());

        LOGGER.info("login the CLI");
        CLIUtils.login(vertx, cli, Environment.PRIMARY_USERNAME, Environment.PRIMARY_PASSWORD).get();

        LOGGER.info("verify that we are logged-in");
        cli.listKafka();
    }

    @SneakyThrows
    public void createServiceAccount() {

        LOGGER.info("create a service account");
        serviceAccountSecret = CLIUtils.createServiceAccount(cli, SERVICE_ACCOUNT_NAME);

        LOGGER.info("get the service account");
        var sa = CLIUtils.getServiceAccountByName(cli, SERVICE_ACCOUNT_NAME);
        LOGGER.debug(sa);

        assertTrue(sa.isPresent());
        assertEquals(sa.get().getName(), SERVICE_ACCOUNT_NAME);
        assertEquals(sa.get().getClientId(), serviceAccountSecret.getClientID());

        serviceAccount = sa.get();
        serviceAccount.setSecret(serviceAccountSecret.getClientSecret());
    }

    @SneakyThrows
    public void createKafkaInstance() {

        LOGGER.info("create kafka instance with name {}", KAFKA_INSTANCE_NAME);
        var k = cli.createKafka(KAFKA_INSTANCE_NAME);
        LOGGER.debug(k);

        LOGGER.info("wait for kafka instance: {}", k.getId());
        kafka = CLIUtils.waitUntilKafkaIsReady(cli, k.getId());
        LOGGER.debug(kafka);
    }

    @SneakyThrows

    @Test(priority = 1)
    public void testCreateAclForUser() {
        LOGGER.info("Create ACL for secondary user");
        cli.createAcl(DIFF_USER, AclOperation.ALL, AclPermissionType.ALLOW, TOPIC_NAME);
        var aclList = cli.listACLs(DIFF_USER);
        Optional<AclBinding> aclFiltered = CLIUtils.searchAcl(aclList, DIFF_USER, AclOperation.ALL, AclPermissionType.ALLOW, AclResourceType.TOPIC, TOPIC_NAME);
        assertTrue(aclFiltered.isPresent());
    }

    @SneakyThrows
    @Test(dependsOnMethods = "testCreateAclForUser", enabled = true)
    public void testDeleteAclForUser() {
        LOGGER.info("Create ACL for secondary user");
        cli.deleteAcl(DIFF_USER, AclOperation.ALL, AclPermissionType.ALLOW);
        var aclList = cli.listACLs(DIFF_USER);
        Optional<AclBinding> aclFiltered = CLIUtils.searchAcl(aclList, DIFF_USER, AclOperation.ALL, AclPermissionType.ALLOW, AclResourceType.TOPIC, TOPIC_NAME);
        assertTrue(aclFiltered.isEmpty());
    }

    @SneakyThrows
    @Test(priority = 2)
    public void testCreateAclForServiceAccount() {
        LOGGER.info("Create ACL for service account");
        cli.createAcl(serviceAccount, AclOperation.ALL, AclPermissionType.ALLOW, TOPIC_NAME);
        var aclList = cli.listACLs(serviceAccount);
        Optional<AclBinding> aclFiltered = CLIUtils.searchAcl(aclList, serviceAccount, AclOperation.ALL, AclPermissionType.ALLOW, AclResourceType.TOPIC, TOPIC_NAME);
        assertTrue(aclFiltered.isPresent());
    }

    @SneakyThrows
    @Test(dependsOnMethods = "testCreateAclForServiceAccount", enabled = true)
    public void testDeleteAclForServiceAccount() {
        LOGGER.info("Delete ACL for service account");
        cli.deleteAcl(serviceAccount, AclOperation.ALL, AclPermissionType.ALLOW);
        var aclList = cli.listACLs(serviceAccount);
        Optional<AclBinding> aclFiltered = CLIUtils.searchAcl(aclList, serviceAccount, AclOperation.ALL, AclPermissionType.ALLOW, AclResourceType.TOPIC, TOPIC_NAME);
        assertTrue(aclFiltered.isEmpty());
    }

    @SneakyThrows
    @Test(priority = 3)
    public void testCreateAclGrantAccessForUser() {
        LOGGER.info("Create grant-access ACL for secondary user");
        cli.grantAccessAcl(DIFF_USER, TOPIC_NAME, "all");
        var aclList = cli.listACLs(DIFF_USER);
        Optional<AclBinding> acl1 = CLIUtils.searchAcl(aclList, DIFF_USER, AclOperation.READ, AclPermissionType.ALLOW, AclResourceType.GROUP, "*");
        Optional<AclBinding> acl2 = CLIUtils.searchAcl(aclList, DIFF_USER, AclOperation.CREATE, AclPermissionType.ALLOW, AclResourceType.TOPIC, TOPIC_NAME);
        Optional<AclBinding> acl3 = CLIUtils.searchAcl(aclList, DIFF_USER, AclOperation.DESCRIBE, AclPermissionType.ALLOW, AclResourceType.TOPIC, TOPIC_NAME);
        Optional<AclBinding> acl4 = CLIUtils.searchAcl(aclList, DIFF_USER, AclOperation.READ, AclPermissionType.ALLOW, AclResourceType.TOPIC, TOPIC_NAME);
        Optional<AclBinding> acl5 = CLIUtils.searchAcl(aclList, DIFF_USER, AclOperation.WRITE, AclPermissionType.ALLOW, AclResourceType.TOPIC, TOPIC_NAME);
        Optional<AclBinding> acl6 = CLIUtils.searchAcl(aclList, DIFF_USER, AclOperation.DESCRIBE, AclPermissionType.ALLOW, AclResourceType.TRANSACTIONAL_ID, "*");
        assertTrue(acl1.isPresent());
        assertTrue(acl2.isPresent());
        assertTrue(acl3.isPresent());
        assertTrue(acl4.isPresent());
        assertTrue(acl5.isPresent());
        assertTrue(acl6.isPresent());
    }

    @SneakyThrows
    @Test(dependsOnMethods = "testCreateAclGrantAccessForUser", enabled = true)
    public void testDeleteAclGrantAccessForUser() {
        LOGGER.info("Delete grant-access ACL for secondary user");
        AclBindingListPage aclList;

        cli.deleteAcl(DIFF_USER, AclOperation.READ, AclPermissionType.ALLOW);
        aclList = cli.listACLs(DIFF_USER);
        Optional<AclBinding> acl1 = CLIUtils.searchAcl(aclList, DIFF_USER, AclOperation.READ, AclPermissionType.ALLOW, AclResourceType.GROUP, "*");
        Optional<AclBinding> acl2 = CLIUtils.searchAcl(aclList, DIFF_USER, AclOperation.READ, AclPermissionType.ALLOW, AclResourceType.TOPIC, TOPIC_NAME);
        assertTrue(acl1.isEmpty());
        assertTrue(acl2.isEmpty());

        cli.deleteAcl(DIFF_USER, AclOperation.DESCRIBE, AclPermissionType.ALLOW);
        aclList = cli.listACLs(DIFF_USER);
        Optional<AclBinding> acl3 = CLIUtils.searchAcl(aclList, DIFF_USER, AclOperation.DESCRIBE, AclPermissionType.ALLOW, AclResourceType.TOPIC, TOPIC_NAME);
        Optional<AclBinding> acl4 = CLIUtils.searchAcl(aclList, DIFF_USER, AclOperation.DESCRIBE, AclPermissionType.ALLOW, AclResourceType.TRANSACTIONAL_ID, "*");
        assertTrue(acl3.isEmpty());
        assertTrue(acl4.isEmpty());

        cli.deleteAllAcls(DIFF_USER);
        aclList = cli.listACLs(DIFF_USER);
        Optional<AclBinding> acl5 = CLIUtils.searchAcl(aclList, DIFF_USER, AclOperation.CREATE, AclPermissionType.ALLOW, AclResourceType.TOPIC, TOPIC_NAME);
        Optional<AclBinding> acl6 = CLIUtils.searchAcl(aclList, DIFF_USER, AclOperation.WRITE, AclPermissionType.ALLOW, AclResourceType.TOPIC, TOPIC_NAME);
        assertTrue(acl5.isEmpty());
        assertTrue(acl6.isEmpty());
    }

    @SneakyThrows
    @Test(priority = 4)
    public void testCreateAclGrantAccessForServiceAccount() {
        LOGGER.info("Create grant-access ACL for secondary service-account");
        cli.grantAccessAcl(serviceAccount, TOPIC_NAME, "all");
        var aclList = cli.listACLs(serviceAccount);
        Optional<AclBinding> acl1 = CLIUtils.searchAcl(aclList, serviceAccount, AclOperation.READ, AclPermissionType.ALLOW, AclResourceType.GROUP, "*");
        Optional<AclBinding> acl2 = CLIUtils.searchAcl(aclList, serviceAccount, AclOperation.CREATE, AclPermissionType.ALLOW, AclResourceType.TOPIC, TOPIC_NAME);
        Optional<AclBinding> acl3 = CLIUtils.searchAcl(aclList, serviceAccount, AclOperation.DESCRIBE, AclPermissionType.ALLOW, AclResourceType.TOPIC, TOPIC_NAME);
        Optional<AclBinding> acl4 = CLIUtils.searchAcl(aclList, serviceAccount, AclOperation.READ, AclPermissionType.ALLOW, AclResourceType.TOPIC, TOPIC_NAME);
        Optional<AclBinding> acl5 = CLIUtils.searchAcl(aclList, serviceAccount, AclOperation.WRITE, AclPermissionType.ALLOW, AclResourceType.TOPIC, TOPIC_NAME);
        Optional<AclBinding> acl6 = CLIUtils.searchAcl(aclList, serviceAccount, AclOperation.DESCRIBE, AclPermissionType.ALLOW, AclResourceType.TRANSACTIONAL_ID, "*");
        assertTrue(acl1.isPresent());
        assertTrue(acl2.isPresent());
        assertTrue(acl3.isPresent());
        assertTrue(acl4.isPresent());
        assertTrue(acl5.isPresent());
        assertTrue(acl6.isPresent());
    }

    @SneakyThrows
    @Test(dependsOnMethods = "testCreateAclGrantAccessForServiceAccount", enabled = true)
    public void testDeleteAclGrantAccessForServiceAccount() {
        LOGGER.info("Delete grant-access ACL for secondary service-account");
        AclBindingListPage aclList;
        
        cli.deleteAcl(serviceAccount, AclOperation.READ, AclPermissionType.ALLOW);
        aclList = cli.listACLs(serviceAccount);
        Optional<AclBinding> acl1 = CLIUtils.searchAcl(aclList, serviceAccount, AclOperation.READ, AclPermissionType.ALLOW, AclResourceType.GROUP, "*");
        Optional<AclBinding> acl2 = CLIUtils.searchAcl(aclList, serviceAccount, AclOperation.READ, AclPermissionType.ALLOW, AclResourceType.TOPIC, TOPIC_NAME);
        assertTrue(acl1.isEmpty());
        assertTrue(acl2.isEmpty());
        
        cli.deleteAcl(serviceAccount, AclOperation.DESCRIBE, AclPermissionType.ALLOW);
        aclList = cli.listACLs(serviceAccount);
        Optional<AclBinding> acl3 = CLIUtils.searchAcl(aclList, serviceAccount, AclOperation.DESCRIBE, AclPermissionType.ALLOW, AclResourceType.TOPIC, TOPIC_NAME);
        Optional<AclBinding> acl4 = CLIUtils.searchAcl(aclList, serviceAccount, AclOperation.DESCRIBE, AclPermissionType.ALLOW, AclResourceType.TRANSACTIONAL_ID, "*");
        assertTrue(acl3.isEmpty());
        assertTrue(acl4.isEmpty());

        cli.deleteAllAcls(serviceAccount);
        aclList = cli.listACLs(serviceAccount);
        Optional<AclBinding> acl5 = CLIUtils.searchAcl(aclList, serviceAccount, AclOperation.CREATE, AclPermissionType.ALLOW, AclResourceType.TOPIC, TOPIC_NAME);
        Optional<AclBinding> acl6 = CLIUtils.searchAcl(aclList, serviceAccount, AclOperation.WRITE, AclPermissionType.ALLOW, AclResourceType.TOPIC, TOPIC_NAME);
        assertTrue(acl5.isEmpty());
        assertTrue(acl6.isEmpty());
    }

    @SneakyThrows
    @Test(priority = 5)
    public void testCreateAclGrantAdminForUser() {
        LOGGER.info("Create grant-access ACL for secondary user");
        cli.grantAdminAcl(DIFF_USER);
        var aclList = cli.listACLs(DIFF_USER);
        Optional<AclBinding> acl = CLIUtils.searchAcl(aclList, DIFF_USER, AclOperation.ALTER, AclPermissionType.ALLOW, AclResourceType.CLUSTER, "kafka-cluster");
        assertTrue(acl.isPresent());
    }

    @SneakyThrows
    @Test(dependsOnMethods = "testCreateAclGrantAdminForUser", enabled = true)
    public void testDeleteAclGrantAdminForUser() {
        LOGGER.info("Delete grant-access ACL for secondary user");
        cli.deleteAcl(DIFF_USER, AclOperation.ALTER, AclPermissionType.ALLOW);
        var aclList = cli.listACLs(DIFF_USER);
        Optional<AclBinding> acl = CLIUtils.searchAcl(aclList, DIFF_USER, AclOperation.ALTER, AclPermissionType.ALLOW, AclResourceType.CLUSTER, "kafka-cluster");
        assertTrue(acl.isEmpty());
    }

    @SneakyThrows
    @Test(priority = 6)
    public void testCreateAclGrantAdminForServiceAccount() {
        LOGGER.info("Create grant-access ACL for secondary service-account");
        cli.grantAdminAcl(serviceAccount);
        var aclList = cli.listACLs(serviceAccount);
        Optional<AclBinding> acl = CLIUtils.searchAcl(aclList, serviceAccount, AclOperation.ALTER, AclPermissionType.ALLOW, AclResourceType.CLUSTER, "kafka-cluster");
        assertTrue(acl.isPresent());
    }

    @SneakyThrows
    @Test(dependsOnMethods = "testCreateAclGrantAdminForServiceAccount", enabled = true)
    public void testDeleteAclGrantAdminForServiceAccount() {
        LOGGER.info("Delete grant-access ACL for secondary service-account");
        cli.deleteAcl(serviceAccount, AclOperation.ALTER, AclPermissionType.ALLOW);
        var aclList = cli.listACLs(serviceAccount);
        Optional<AclBinding> acl = CLIUtils.searchAcl(aclList, serviceAccount, AclOperation.ALTER, AclPermissionType.ALLOW, AclResourceType.CLUSTER, "kafka-cluster");
        assertTrue(acl.isEmpty());
    }
}
