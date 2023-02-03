package io.managed.services.test.client.registrymgmt;
import com.openshift.cloud.api.srs.models.RootTypeForRegistry;

public class RegistryNotReadyException extends Exception {

    public RegistryNotReadyException(RootTypeForRegistry r, Exception cause) {
        super("registry not ready: \n" + r.toString(), cause);
    }
}
