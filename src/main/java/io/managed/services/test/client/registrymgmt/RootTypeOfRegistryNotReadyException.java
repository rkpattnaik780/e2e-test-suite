package io.managed.services.test.client.registrymgmt;
import com.openshift.cloud.api.srs.models.RootTypeForRegistry;

public class RootTypeOfRegistryNotReadyException extends Exception {

    public RootTypeOfRegistryNotReadyException(RootTypeForRegistry registry, Exception cause) {
        super("registry not ready: \n" + registry.toString(), cause);
    }
}
