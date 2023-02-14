package io.managed.services.test.rhoas;

import com.openshift.cloud.api.srs.models.Registry;
import io.managed.services.test.Environment;
import io.managed.services.test.cli.CLI;
import io.managed.services.test.cli.CLIDownloader;
import io.managed.services.test.cli.CLIUtils;
import io.managed.services.test.client.registrymgmt.RegistryMgmtApiUtils;
import io.vertx.core.Vertx;
import lombok.SneakyThrows;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;

import static io.managed.services.test.TestUtils.bwait;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

/**
 * Test the application services CLI[1] service-registry commands.
 * <p>
 * The tests download the CLI from GitHub to the local machine where the test suite is running
 * and perform all operations using the CLI.
 * <p>
 * <b>Requires:</b>
 * <ul>
 *     <li> PRIMARY_OFFLINE_TOKEN
 *     <li> PRIMARY_USERNAME
 *     <li> PRIMARY_PASSWORD
 * </ul>
 */
@Test
public class KafkaRhoasRegistryTests {
    private static final Logger LOGGER = LogManager.getLogger(KafkaRhoasRegistryTests.class);

    private static final String SERVICE_REGISTRY_NAME = "e2e-cli-registry-"  + Environment.LAUNCH_SUFFIX;

    private final Vertx vertx = Vertx.vertx();

    private CLI cli;

    private Registry registry;

    @BeforeClass
    public void bootstrap() throws Throwable {
        assertNotNull(Environment.PRIMARY_USERNAME, "the PRIMARY_USERNAME env is null");
        assertNotNull(Environment.PRIMARY_PASSWORD, "the PRIMARY_PASSWORD env is null");
        assertNotNull(Environment.PRIMARY_OFFLINE_TOKEN, "the PRIMARY_OFFLINE_TOKEN env is null");

        LOGGER.info("download cli");
        var downloader = CLIDownloader.defaultDownloader();
        var binary = downloader.downloadCLIInTempDir();
        this.cli = new CLI(binary);

        LOGGER.info("login to RHOAS CLI with help of logging to the browser");
        CLIUtils.login(vertx, cli, Environment.PRIMARY_USERNAME, Environment.PRIMARY_PASSWORD).get();
    }

    @AfterClass(alwaysRun = true)
    @SneakyThrows
    public void clean() {
        var registryMgmtApi = RegistryMgmtApiUtils.registryMgmtApi(Environment.OPENSHIFT_API_URI, Environment.PRIMARY_OFFLINE_TOKEN);

        try {
            RegistryMgmtApiUtils.deleteRegistryByNameIfExists(registryMgmtApi, SERVICE_REGISTRY_NAME);
        } catch (Throwable t) {
            LOGGER.error("delete registry instance error: ", t);
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

    @Test
    @SneakyThrows
    public void testCreateServiceRegistry() {
        LOGGER.info("create service registry instance with name {}", SERVICE_REGISTRY_NAME);
        var r = cli.createServiceRegistry(SERVICE_REGISTRY_NAME);
        LOGGER.debug(r);

        LOGGER.info("wait for service registry instance with name: {}, with id: {}", r.getName(), r.getId());
        registry = CLIUtils.waitUntilServiceRegistryIsReady(cli, r.getId());
        LOGGER.debug(registry);
    }

    @Test(dependsOnMethods = "testCreateServiceRegistry")
    @SneakyThrows
    public void testDescribeServiceRegistry() {
        LOGGER.info("describe service registry instance with with name {}", SERVICE_REGISTRY_NAME);
        var r = cli.describeServiceRegistry(registry.getId());
        LOGGER.debug(r);

        assertEquals("ready", r.getStatus().getValue());
    }

    @Test(dependsOnMethods = "testCreateServiceRegistry")
    @SneakyThrows
    public void testListServiceRegistry() {
        var list = cli.listServiceRegistry();
        LOGGER.debug(list);

        var exists = list.getItems().stream()
            .filter(r -> SERVICE_REGISTRY_NAME.equals(r.getName()))
            .findAny();
        assertTrue(exists.isPresent());
    }

    @Test(dependsOnMethods = "testCreateServiceRegistry")
    @SneakyThrows
    public void testUseServiceRegistry() {
        LOGGER.info("use service registry instance with id {}", registry.getId());
        cli.useServiceRegistry(registry.getId());
        var r = cli.describeServiceRegistry();
        assertEquals(r.getId(), registry.getId());
    }

    @Test(dependsOnMethods = "testCreateServiceRegistry", priority = 1)
    @SneakyThrows
    public void testDeleteServiceRegistry() {
        LOGGER.info("delete service registry instance with id {}", registry.getId());

        cli.deleteServiceRegistry(registry.getId());
        CLIUtils.waitUntilRegistryIsDeleted(cli, registry.getId());
    }
}
