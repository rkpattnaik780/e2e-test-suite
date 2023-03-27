package io.managed.services.test.rhoas;

import com.openshift.cloud.api.kas.auth.models.AclBinding;
import com.openshift.cloud.api.kas.auth.models.AclBindingListPage;
import com.openshift.cloud.api.kas.auth.models.AclOperation;
import com.openshift.cloud.api.kas.auth.models.AclPermissionType;
import com.openshift.cloud.api.kas.auth.models.AclResourceType;
import io.managed.services.test.Environment;
import io.managed.services.test.TestBase;
import io.managed.services.test.cli.CLI;
import io.managed.services.test.cli.CLIDownloader;
import io.managed.services.test.cli.CLIUtils;
import io.managed.services.test.cli.CLI.ACLEntityType;
import io.managed.services.test.client.kafkamgmt.KafkaMgmtApiUtils;
import io.managed.services.test.client.securitymgmt.SecurityMgmtAPIUtils;
import io.vertx.core.Vertx;
import lombok.SneakyThrows;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.File;
import java.util.Optional;

import static io.managed.services.test.TestUtils.bwait;
import static org.testng.Assert.assertNotNull;
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
 *     <li> PRIMARY_USERNAME
 *     <li> PRIMARY_PASSWORD
 * </ul>
 */
@Test
public class KafkaRhoasAclTests extends TestBase {

    private static final Logger LOGGER = LogManager.getLogger(KafkaRhoasAclTests.class);
    
    private static final String KAFKA_INSTANCE_NAME = "e2e-cli-acl-" + Environment.LAUNCH_SUFFIX;
    private static final String SERVICE_ACCOUNT_NAME = "e2e-cli-acl-svc-acc-"  + Environment.LAUNCH_SUFFIX;
    private static final String TOPIC_NAME = "e2e-cli-acl-topic" + Environment.LAUNCH_SUFFIX;
    private static final String EXTERNAL_USER = "external-user";

    private static final String ALL_RESOURCES = "*";
    private static final String KAFKA_CLUSTER = "kafka-cluster";

    private CLI cli;

    private final Vertx vertx = Vertx.vertx();
    
    @BeforeClass
    @SneakyThrows
    public void bootstrap() {
        assertNotNull(Environment.PRIMARY_USERNAME, "the PRIMARY_USERNAME env is null");
        assertNotNull(Environment.PRIMARY_PASSWORD, "the PRIMARY_PASSWORD env is null");
        assertNotNull(Environment.PRIMARY_OFFLINE_TOKEN, "the PRIMARY_OFFLINE_TOKEN env is null");

        // download CLI
        var downloader = CLIDownloader.defaultDownloader();
        var binary = downloader.downloadCLIInTempDir();
        this.cli = new CLI(binary);
        LOGGER.debug(cli.help());

        // login
        LOGGER.info("login the CLI");
        CLIUtils.login(vertx, cli, Environment.PRIMARY_USERNAME, Environment.PRIMARY_PASSWORD).get();
        cli.listKafka();

        // apply kafka instance
        LOGGER.info("apply kafka instance with name {}", KAFKA_INSTANCE_NAME);
        var k = CLIUtils.applyKafkaInstance(cli, KAFKA_INSTANCE_NAME);
        LOGGER.debug(k);

    }

    @AfterClass(alwaysRun = true)
    @SneakyThrows
    public void clean() {
        var kafkaMgmtApi =  KafkaMgmtApiUtils.kafkaMgmtApi(Environment.OPENSHIFT_API_URI, Environment.PRIMARY_OFFLINE_TOKEN);
        var securityMgmtApi = SecurityMgmtAPIUtils.securityMgmtApi(Environment.OPENSHIFT_API_URI, Environment.PRIMARY_OFFLINE_TOKEN);
        
        if (Environment.SKIP_KAFKA_TEARDOWN) {
            LOGGER.warn("skip kafka instance clean up");
            return;
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
    
    @DataProvider(name = "aclEntityPairs")
    public Object[][] aclEntityPairsDataProvider() {
        return new Object[][]{
            {ACLEntityType.USER, EXTERNAL_USER},
            {ACLEntityType.SERVICE_ACCOUNT, SERVICE_ACCOUNT_NAME}
        };
    }

    // Primary user creates specific ACL (details not important) which allows entity to perform all operation upon specific topic
    // Outcome: ACL assigned to entity is created and can be found once it is searched for based on entity
    @SneakyThrows
    @Test(dataProvider = "aclEntityPairs")
    public void testCreateAllAllowAcl(ACLEntityType entityType, String entityIdentificator) {
        LOGGER.info("Create ACL for {}", entityType.name);
        cli.createAcl(entityType, entityIdentificator, AclOperation.ALL, AclPermissionType.ALLOW, TOPIC_NAME);
        var aclList = cli.listACLs(entityType, entityIdentificator);
        Optional<AclBinding> aclFiltered = CLIUtils.searchAcl(aclList, entityIdentificator, AclOperation.ALL, AclPermissionType.ALLOW, AclResourceType.TOPIC, TOPIC_NAME);
        assertTrue(aclFiltered.isPresent(), "ACL created must exist");
        cli.deleteAllAcls(entityType, entityIdentificator);
    }

    // Primary user deleted specific ACL (details not important) upon specific topic
    // Outcome: ACL assigned to entity is remove and can NOT be found once it is searched for based on entity
    @SneakyThrows
    @Test(dataProvider = "aclEntityPairs")
    public void testDeleteAllAllowAcl(ACLEntityType entityType, String entityIdentificator) {
        LOGGER.info("Delete ACL for {}", entityType.name);
        cli.createAcl(entityType, entityIdentificator, AclOperation.ALL, AclPermissionType.ALLOW, TOPIC_NAME);
        cli.deleteAllAcls(entityType, entityIdentificator);
        var aclList = cli.listACLs(entityType, entityIdentificator);
        Optional<AclBinding> aclFiltered = CLIUtils.searchAcl(aclList, entityIdentificator, AclOperation.ALL, AclPermissionType.ALLOW, AclResourceType.TOPIC, TOPIC_NAME);
        assertTrue(aclFiltered.isEmpty(), "ACL deleted must be removed");
    }

    // Primary user creates six ACLs using grant-access permission to produce and consume records on a topic
    // Outcome: Six ACLs assigned to entity are created and can be found once it is searched for based on entity
    @SneakyThrows
    @Test(dataProvider = "aclEntityPairs")
    public void testCreateAclGrantAccess(ACLEntityType entityType, String entityIdentificator) {
        LOGGER.info("Create grant-access ACL for {}", entityType.name);
        cli.grantAccessAcl(entityType, entityIdentificator, TOPIC_NAME, "all");
        var aclList = cli.listACLs(entityType, entityIdentificator);
        Optional<AclBinding> acl1 = CLIUtils.searchAcl(aclList, entityIdentificator, AclOperation.READ, AclPermissionType.ALLOW, AclResourceType.GROUP, ALL_RESOURCES);
        Optional<AclBinding> acl2 = CLIUtils.searchAcl(aclList, entityIdentificator, AclOperation.CREATE, AclPermissionType.ALLOW, AclResourceType.TOPIC, TOPIC_NAME);
        Optional<AclBinding> acl3 = CLIUtils.searchAcl(aclList, entityIdentificator, AclOperation.DESCRIBE, AclPermissionType.ALLOW, AclResourceType.TOPIC, TOPIC_NAME);
        Optional<AclBinding> acl4 = CLIUtils.searchAcl(aclList, entityIdentificator, AclOperation.READ, AclPermissionType.ALLOW, AclResourceType.TOPIC, TOPIC_NAME);
        Optional<AclBinding> acl5 = CLIUtils.searchAcl(aclList, entityIdentificator, AclOperation.WRITE, AclPermissionType.ALLOW, AclResourceType.TOPIC, TOPIC_NAME);
        Optional<AclBinding> acl6 = CLIUtils.searchAcl(aclList, entityIdentificator, AclOperation.DESCRIBE, AclPermissionType.ALLOW, AclResourceType.TRANSACTIONAL_ID, ALL_RESOURCES);
        assertTrue(acl1.isPresent(), "ACL created must exist");
        assertTrue(acl2.isPresent(), "ACL created must exist");
        assertTrue(acl3.isPresent(), "ACL created must exist");
        assertTrue(acl4.isPresent(), "ACL created must exist");
        assertTrue(acl5.isPresent(), "ACL created must exist");
        assertTrue(acl6.isPresent(), "ACL created must exist");
        cli.deleteAllAcls(entityType, entityIdentificator);
    }

    // Primary user deletes six ACLs to remove grant-access permission to produce and consume records on a topic
    // Outcome: ACLs assigned to entity are deleted and can NOT be found once it is searched for based on entity
    @SneakyThrows
    @Test(dataProvider = "aclEntityPairs")
    public void testDeleteAclGrantAccess(ACLEntityType entityType, String entityIdentificator) {
        LOGGER.info("Delete grant-access ACL for {}", entityType.name);
        cli.grantAccessAcl(entityType, entityIdentificator, TOPIC_NAME, "all");
        cli.deleteAcl(entityType, entityIdentificator, AclOperation.READ, AclPermissionType.ALLOW);
        AclBindingListPage aclList = cli.listACLs(entityType, entityIdentificator);
        Optional<AclBinding> acl1 = CLIUtils.searchAcl(aclList, entityIdentificator, AclOperation.READ, AclPermissionType.ALLOW, AclResourceType.GROUP, ALL_RESOURCES);
        Optional<AclBinding> acl2 = CLIUtils.searchAcl(aclList, entityIdentificator, AclOperation.READ, AclPermissionType.ALLOW, AclResourceType.TOPIC, TOPIC_NAME);
        Optional<AclBinding> acl3 = CLIUtils.searchAcl(aclList, entityIdentificator, AclOperation.DESCRIBE, AclPermissionType.ALLOW, AclResourceType.TOPIC, TOPIC_NAME);
        Optional<AclBinding> acl4 = CLIUtils.searchAcl(aclList, entityIdentificator, AclOperation.DESCRIBE, AclPermissionType.ALLOW, AclResourceType.TRANSACTIONAL_ID, ALL_RESOURCES);
        Optional<AclBinding> acl5 = CLIUtils.searchAcl(aclList, entityIdentificator, AclOperation.CREATE, AclPermissionType.ALLOW, AclResourceType.TOPIC, TOPIC_NAME);
        Optional<AclBinding> acl6 = CLIUtils.searchAcl(aclList, entityIdentificator, AclOperation.WRITE, AclPermissionType.ALLOW, AclResourceType.TOPIC, TOPIC_NAME);
        assertTrue(acl1.isEmpty(), "ACL deleted must be removed");
        assertTrue(acl2.isEmpty(), "ACL deleted must be removed");
        assertTrue(acl3.isPresent(), "ACL deleted must exist");
        assertTrue(acl4.isPresent(), "ACL deleted must exist");
        assertTrue(acl5.isPresent(), "ACL deleted must exist");
        assertTrue(acl6.isPresent(), "ACL deleted must exist");
        cli.deleteAllAcls(entityType, entityIdentificator);
    }

    // Primary user creates one specific ACL using grant-admin permission
    // Outcome: One ACL assigned to entity is created and can be found once it is searched for based on entity
    @SneakyThrows
    @Test(dataProvider = "aclEntityPairs")
    public void testCreateAclGrantAdmin(ACLEntityType entityType, String entityIdentificator) {
        LOGGER.info("Create grant-access ACL for {}", entityType.name);
        cli.grantAdminAcl(entityType, entityIdentificator);
        var aclList = cli.listACLs(entityType, entityIdentificator);
        Optional<AclBinding> acl = CLIUtils.searchAcl(aclList, entityIdentificator, AclOperation.ALTER, AclPermissionType.ALLOW, AclResourceType.CLUSTER, KAFKA_CLUSTER);
        assertTrue(acl.isPresent(), "ACL created must exist");
        cli.deleteAllAcls(entityType, entityIdentificator);
    }
}
