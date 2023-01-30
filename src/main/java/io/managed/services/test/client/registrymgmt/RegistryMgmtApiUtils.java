package io.managed.services.test.client.registrymgmt;

import com.openshift.cloud.api.srs.invoker.ApiClient;
import com.openshift.cloud.api.srs.models.Registry;
import com.openshift.cloud.api.srs.models.RegistryCreate;
import com.openshift.cloud.api.srs.models.RegistryList;
import io.managed.services.test.Environment;
import io.managed.services.test.ThrowingFunction;
import io.managed.services.test.ThrowingSupplier;
import io.managed.services.test.client.exception.ApiGenericException;
import io.managed.services.test.client.exception.ApiNotFoundException;
import io.vertx.core.json.Json;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Optional;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static io.managed.services.test.TestUtils.waitFor;
import static java.time.Duration.ofMinutes;
import static java.time.Duration.ofSeconds;

public class RegistryMgmtApiUtils {
    private static final Logger LOGGER = LogManager.getLogger(RegistryMgmtApiUtils.class);

    public static RegistryMgmtApi registryMgmtApi(String offlineToken) {
        return new RegistryMgmtApi(new ApiClient().setBasePath(Environment.OPENSHIFT_API_URI), offlineToken);
    }
    public static RegistryMgmtApi registryMgmtApi(String uri, String offlineToken) {
        return new RegistryMgmtApi(new ApiClient().setBasePath(uri), offlineToken);
    }

    /**
     * Create a Registry using the default options if it doesn't exist or return the existing Registry
     *
     * @param api  RegistryMgmtApi
     * @param name Name for the Registry
     * @return Registry
     */
    public static Registry applyRegistry(RegistryMgmtApi api, String name)
        throws ApiGenericException, InterruptedException, RegistryNotReadyException {

        var registryCreateRest = new RegistryCreate().name(name);
        return applyRegistry(api, registryCreateRest);
    }

    /**
     * Create a Registry if it doesn't exist or return the existing Registry
     *
     * @param api     RegistryMgmtApi
     * @param payload RegistryCreate
     * @return Registry
     */
    public static Registry applyRegistry(RegistryMgmtApi api, RegistryCreate payload)
        throws ApiGenericException, InterruptedException,  RegistryNotReadyException {

        var registryList = getRegistryByName(api, payload.getName());

        if (registryList.getItems().size() > 0) {
            var registry = registryList.getItems().get(0);
            LOGGER.warn("registry already exists: {}", Json.encode(registry));
            return registry;
        }

        LOGGER.info("create registry: {}", payload.getName());
        var registry = api.createRegistry(payload);

        registry = waitUntilRegistryIsReady(api, registry.getId());

        LOGGER.info("registry ready: {}", Json.encode(registry));
        return registry;
    }

    /**
     * Function that returns Registry only if status is in ready
     *
     * @param api        RegistryMgmtApi
     * @param registryID String
     * @return Registry
     */
    public static Registry waitUntilRegistryIsReady(RegistryMgmtApi api, String registryID)
        throws RegistryNotReadyException, InterruptedException, ApiGenericException {

        return waitUntilRegistryIsReady(() -> api.getRegistry(registryID));
    }

    public static <T extends Throwable> Registry waitUntilRegistryIsReady(ThrowingSupplier<Registry, T> supplier)
            throws T, InterruptedException, RegistryNotReadyException {

        var registryAtom = new AtomicReference<Registry>();
        ThrowingFunction<Boolean, Boolean, T> ready = last -> {
            var registry = supplier.get();
            registryAtom.set(registry);

            LOGGER.debug(registry);
            return "ready".equals(registry.getStatus().getValue());
        };

        try {
            waitFor("registry to be ready", ofSeconds(5), ofMinutes(1), ready);
        } catch (TimeoutException e) {
            // throw a more accurate error
            throw new RegistryNotReadyException(registryAtom.get(), e);
        }

        var registry = registryAtom.get();
        LOGGER.info("service registry '{}' is ready", registry.getName());
        LOGGER.debug(registry);
        return registry;
    }

    public static void cleanRegistry(RegistryMgmtApi api, String name) throws ApiGenericException {
        deleteRegistryByNameIfExists(api, name);
    }

    public static void deleteRegistryByNameIfExists(RegistryMgmtApi api, String name) throws ApiGenericException {

        // Attention: this deletes all registries with the given name
        var registries = getRegistryByName(api, name);

        if (registries.getItems().isEmpty()) {
            LOGGER.warn("registry '{}' not found", name);
        }

        // TODO: refactor after the names are unique: https://github.com/bf2fc6cc711aee1a0c2a/srs-fleet-manager/issues/75
        for (var r : registries.getItems()) {
            LOGGER.info("delete registry: {}", r.getId());
            api.deleteRegistry(r.getId());
        }
    }

    public static void waitUntilRegistryIsDeleted(RegistryMgmtApi api, String registryId)
        throws InterruptedException, ApiGenericException, RegistryNotDeletedException {

        waitUntilRegistryIsDeleted(() -> {
            try {
                return Optional.of(api.getRegistry(registryId));
            } catch (ApiNotFoundException __) {
                return Optional.empty();
            }
        });
    }

    /**
     * Return only if the Registry instance is deleted
     *
     * @param supplier Return true if the instance doesn't exist anymore
     */
    public static <T extends Throwable> void waitUntilRegistryIsDeleted(
        ThrowingSupplier<Optional<Registry>, T> supplier)
        throws T, InterruptedException, RegistryNotDeletedException {

        var registryAtom = new AtomicReference<Registry>();
        ThrowingFunction<Boolean, Boolean, T> ready = l -> {
            var exists = supplier.get();
            if (exists.isEmpty()) {
                return true;
            }

            var registry = exists.get();
            LOGGER.debug(registry);
            registryAtom.set(registry);
            return false;
        };

        try {
            waitFor("registry instance to be deleted", ofSeconds(1), ofSeconds(20), ready);
        } catch (TimeoutException e) {
            throw new RegistryNotDeletedException(registryAtom.get(), e);
        }
    }


    public static RegistryList getRegistryByName(RegistryMgmtApi api, String name) throws ApiGenericException {

        // Attention: we support only 10 registries until the name doesn't become unique
        return api.getRegistries(1, 10, null, String.format("name = %s", name));
    }
}
