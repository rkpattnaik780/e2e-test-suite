package io.managed.services.test.registry;

import com.openshift.cloud.api.srs.models.Registry;
import io.managed.services.test.Environment;
import io.managed.services.test.TestBase;
import io.managed.services.test.TestUtils;
import io.managed.services.test.client.exception.ApiForbiddenException;
import io.managed.services.test.client.exception.ApiGenericException;
import io.managed.services.test.client.exception.ApiNotFoundException;
import io.managed.services.test.client.exception.ApiUnauthorizedException;
import io.managed.services.test.client.oauth.KeycloakUser;
import io.managed.services.test.client.registrymgmt.RegistryMgmtApi;
import io.managed.services.test.client.registrymgmt.RegistryMgmtApiUtils;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;

import static io.managed.services.test.TestUtils.assumeTeardown;
import static io.managed.services.test.TestUtils.bwait;
import static io.managed.services.test.client.registry.RegistryClientUtils.registryClient;
import static io.managed.services.test.client.registrymgmt.RegistryMgmtApiUtils.applyRegistry;
import static io.managed.services.test.client.registrymgmt.RegistryMgmtApiUtils.cleanRegistry;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertThrows;

/**
 * Test the User authn and authz for the Registry Mgmt API.
 * <p>
 * <b>Requires:</b>
 * <ul>
 *     <li> PRIMARY_USERNAME
 *     <li> PRIMARY_PASSWORD
 *     <li> SECONDARY_USERNAME
 *     <li> SECONDARY_PASSWORD
 *     <li> ALIEN_USERNAME
 *     <li> ALIEN_PASSWORD
 *     <li> ADMIN_USERNAME
 *     <li> ADMIN_PASSWORD
 * </ul>
 */
public class RegistryMgmtAPIPermissionsTest extends TestBase {
    private static final Logger LOGGER = LogManager.getLogger(RegistryMgmtAPIPermissionsTest.class);

    private static final String SERVICE_REGISTRY_NAME = "mk-e2e-sr-rmp-" + Environment.LAUNCH_KEY + Environment.CLOUD_PROVIDER;
    private static final String ARTIFACT_SCHEMA = "{\"type\":\"record\",\"name\":\"Greeting\",\"fields\":[{\"name\":\"Message\",\"type\":\"string\"},{\"name\":\"Time\",\"type\":\"long\"}]}";

    private final Vertx vertx = Vertx.vertx();

    private RegistryMgmtApi adminRegistryMgmtApi;
    private RegistryMgmtApi registryMgmtApi;
    private RegistryMgmtApi secondaryRegistryMgmtApi;
    private RegistryMgmtApi alienRegistryMgmtApi;

    private Registry registry;

    @BeforeClass
    public void bootstrap() throws Throwable {
        assertNotNull(Environment.ADMIN_USERNAME, "the ADMIN_USERNAME env is null");
        assertNotNull(Environment.ADMIN_PASSWORD, "the ADMIN_PASSWORD env is null");
        assertNotNull(Environment.PRIMARY_USERNAME, "the PRIMARY_USERNAME env is null");
        assertNotNull(Environment.PRIMARY_PASSWORD, "the PRIMARY_PASSWORD env is null");
        assertNotNull(Environment.SECONDARY_USERNAME, "the SECONDARY_USERNAME env is null");
        assertNotNull(Environment.SECONDARY_PASSWORD, "the SECONDARY_PASSWORD env is null");
        assertNotNull(Environment.ALIEN_USERNAME, "the ALIEN_USERNAME env is null");
        assertNotNull(Environment.ALIEN_PASSWORD, "the ALIEN_PASSWORD env is null");

        adminRegistryMgmtApi = bwait(RegistryMgmtApiUtils.registryMgmtApi(
            Environment.ADMIN_USERNAME,
            Environment.ADMIN_PASSWORD));

        registryMgmtApi = bwait(RegistryMgmtApiUtils.registryMgmtApi(
            Environment.PRIMARY_USERNAME,
            Environment.PRIMARY_PASSWORD));

        secondaryRegistryMgmtApi = bwait(RegistryMgmtApiUtils.registryMgmtApi(
            Environment.SECONDARY_USERNAME,
            Environment.SECONDARY_PASSWORD));

        alienRegistryMgmtApi = bwait(RegistryMgmtApiUtils.registryMgmtApi(
            Environment.ALIEN_USERNAME,
            Environment.ALIEN_PASSWORD));

        registry = applyRegistry(registryMgmtApi, SERVICE_REGISTRY_NAME);
    }

    @AfterClass(alwaysRun = true)
    public void teardown() throws Throwable {
        assumeTeardown();

        try {
            if (registryMgmtApi != null) {
                cleanRegistry(registryMgmtApi, SERVICE_REGISTRY_NAME);
            }
        } catch (Throwable t) {
            LOGGER.error("clean service registry error: ", t);
        }

        bwait(vertx.close());
    }

    @Test
    public void testSecondaryUserCanReadTheRegistry() throws ApiGenericException {
        var r = secondaryRegistryMgmtApi.getRegistry(registry.getId());
        assertEquals(r.getName(), registry.getName());
    }

    @Test
    public void testUserCanReadTheRegistry() throws ApiGenericException {
        LOGGER.info("registries: {}", Json.encode(registryMgmtApi.getRegistries(null, null, null, null)));
        LOGGER.info("registry: {}", Json.encode(registry));
        var r = registryMgmtApi.getRegistry(registry.getId());
        assertEquals(r.getName(), registry.getName());
    }

    @Test
    public void testAlienUserCanNotReadTheRegistry() {
        assertThrows(ApiNotFoundException.class, () -> alienRegistryMgmtApi.getRegistry(registry.getId()));
    }

    @Test
    public void testAlienUserCanNotCreateArtifactOnTheRegistry() throws Throwable {
        var registryClient = bwait(registryClient(vertx, registry.getRegistryUrl(),
            Environment.ALIEN_USERNAME,
            Environment.ALIEN_PASSWORD));

        assertThrows(ApiForbiddenException.class, () -> registryClient.createArtifact(null, null, ARTIFACT_SCHEMA.getBytes(StandardCharsets.UTF_8)));
    }

    @Test(priority = 1)
    public void testSecondaryUserCanNotDeleteTheRegistry() {
        assertThrows(ApiForbiddenException.class, () -> secondaryRegistryMgmtApi.deleteRegistry(registry.getId()));
    }

    @Test(priority = 1)
    public void testAlienUserCanNotDeleteTheRegistry() {
        assertThrows(ApiForbiddenException.class, () -> alienRegistryMgmtApi.deleteRegistry(registry.getId()));
    }

    @Test
    public void testUnauthenticatedUserWithFakeToken() {
        var api = RegistryMgmtApiUtils.registryMgmtApi(Environment.OPENSHIFT_API_URI, new KeycloakUser(TestUtils.FAKE_TOKEN));
        assertThrows(ApiUnauthorizedException.class, () -> api.getRegistries(null, null, null, null));
    }

    @Test
    public void testUnauthenticatedUserWithoutToken() {
        var api = RegistryMgmtApiUtils.registryMgmtApi(Environment.OPENSHIFT_API_URI, new KeycloakUser(""));
        assertThrows(ApiUnauthorizedException.class, () -> api.getRegistries(null, null, null, null));
    }

    @Test
    public void testAdminUserCanCreateArtifactOnTheRegistry() throws Throwable {
        var registryClient = bwait(registryClient(vertx, registry.getRegistryUrl(),
            Environment.ADMIN_USERNAME,
            Environment.ADMIN_PASSWORD));

        registryClient.createArtifact(null, null, ARTIFACT_SCHEMA.getBytes(StandardCharsets.UTF_8));
    }

    @Test(priority = 2)
    public void testAdminUserCanDeleteTheRegistry() throws Throwable {
        // deletion of register by admin
        adminRegistryMgmtApi.deleteRegistry(registry.getId());

    }
}
