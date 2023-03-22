package io.managed.services.test.client.registrymgmt;
import com.openshift.cloud.api.srs.models.Registry;

public class RegistryNotReadyException extends Exception {

    public RegistryNotReadyException(Registry registry, Exception cause) {
        super("registry not ready: \n" + registry.toString(), cause);
    }
}
