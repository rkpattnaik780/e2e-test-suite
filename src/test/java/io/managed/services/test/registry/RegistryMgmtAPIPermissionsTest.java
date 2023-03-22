package io.managed.services.test.registry;

import com.openshift.cloud.api.registry.instance.models.ContentCreateRequest;
import com.openshift.cloud.api.srs.models.RootTypeForRegistry;
import io.managed.services.test.Environment;
import io.managed.services.test.TestBase;
import io.managed.services.test.TestUtils;
import io.managed.services.test.client.exception.ApiGenericException;
import io.managed.services.test.client.registry.RegistryClient;
import io.managed.services.test.client.registrymgmt.RegistryMgmtApi;
import io.managed.services.test.client.registrymgmt.RegistryMgmtApiUtils;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
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
 *     <li> PRIMARY_OFFLINE_TOKEN
 *     <li> SECONDARY_OFFLINE_TOKEN
 *     <li> ALIEN_OFFLINE_TOKEN
 *     <li> ADMIN_OFFLINE_TOKEN
 * </ul>
 */
public class RegistryMgmtAPIPermissionsTest extends TestBase {
    private static final Logger LOGGER = LogManager.getLogger(RegistryMgmtAPIPermissionsTest.class);

    private static final String SERVICE_REGISTRY_NAME = "rama-test";
    private static final String ARTIFACT_SCHEMA = "{\"type\":\"record\",\"name\":\"Greeting\",\"fields\":[{\"name\":\"Message\",\"type\":\"string\"},{\"name\":\"Time\",\"type\":\"long\"}]}";

    private final Vertx vertx = Vertx.vertx();

    private RegistryMgmtApi adminRegistryMgmtApi;
    private RegistryMgmtApi registryMgmtApi;
    private RegistryMgmtApi secondaryRegistryMgmtApi;
    private RegistryMgmtApi alienRegistryMgmtApi;

    private RootTypeForRegistry registry;

    @BeforeClass
    public void bootstrap() throws Throwable {
        assertNotNull(Environment.PRIMARY_OFFLINE_TOKEN, "the PRIMARY_OFFLINE_TOKEN env is null");
        assertNotNull(Environment.SECONDARY_OFFLINE_TOKEN, "the ADMIN_OFFLINE_TOKEN env is null");
        assertNotNull(Environment.ADMIN_OFFLINE_TOKEN, "the ADMIN_OFFLINE_TOKEN env is null");
        assertNotNull(Environment.ALIEN_OFFLINE_TOKEN, "the ALIEN_OFFLINE_TOKEN env is null");

        adminRegistryMgmtApi = RegistryMgmtApiUtils.registryMgmtApi(Environment.ADMIN_OFFLINE_TOKEN);
        registryMgmtApi = RegistryMgmtApiUtils.registryMgmtApi(Environment.PRIMARY_OFFLINE_TOKEN);
        secondaryRegistryMgmtApi = RegistryMgmtApiUtils.registryMgmtApi(Environment.SECONDARY_OFFLINE_TOKEN);
        alienRegistryMgmtApi = RegistryMgmtApiUtils.registryMgmtApi(Environment.ALIEN_OFFLINE_TOKEN);

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
        assertThrows(ApiGenericException.class, () -> alienRegistryMgmtApi.getRegistry(registry.getId()));
    }

    @Test
    public void testAlienUserCanNotCreateArtifactOnTheRegistry() throws Throwable {
        RegistryClient registryClient = registryClient(registry.getRegistryUrl(), Environment.ALIEN_OFFLINE_TOKEN);
        var content = new ContentCreateRequest();
        content.setContent(ARTIFACT_SCHEMA);
        assertThrows(ApiGenericException.class, () -> registryClient.createArtifact(content));
    }

    @Test(priority = 1)
    public void testSecondaryUserCanNotDeleteTheRegistry() {
        assertThrows(ApiGenericException.class, () -> secondaryRegistryMgmtApi.deleteRegistry(registry.getId()));
    }

    @Test(priority = 1)
    public void testAlienUserCanNotDeleteTheRegistry() {
        assertThrows(ApiGenericException.class, () -> alienRegistryMgmtApi.deleteRegistry(registry.getId()));
    }

    @Test
    public void testUnauthenticatedUserWithFakeToken() {
        var api = RegistryMgmtApiUtils.registryMgmtApi(Environment.OPENSHIFT_API_URI, TestUtils.FAKE_TOKEN);
        assertThrows(Exception.class, () -> api.getRegistries(null, null, null, null));
    }

    @Test
    public void testAdminUserCanCreateArtifactOnTheRegistry() throws Throwable {
        var registryClient = registryClient(registry.getRegistryUrl(), Environment.ADMIN_OFFLINE_TOKEN);
        var content = new ContentCreateRequest();
        content.setContent(ARTIFACT_SCHEMA);
        registryClient.createArtifact(content);

    }

    @Test(priority = 2)
    public void testAdminUserCanDeleteTheRegistry() throws Throwable {
        // deletion of register by admin
        adminRegistryMgmtApi.deleteRegistry(registry.getId());

    }
}
