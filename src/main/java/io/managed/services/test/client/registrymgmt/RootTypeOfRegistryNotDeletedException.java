package io.managed.services.test.client.registrymgmt;
import com.openshift.cloud.api.srs.models.RootTypeForRegistry;

public class RootTypeOfRegistryNotDeletedException extends Exception {

    public RootTypeOfRegistryNotDeletedException(RootTypeForRegistry rootTypeForRegistry, Exception cause) {
        super("registry instance is not deleted\n" + rootTypeForRegistry.toString(), cause);
    }
}
