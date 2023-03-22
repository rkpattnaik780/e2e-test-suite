package io.managed.services.test.client.registrymgmt;
import com.openshift.cloud.api.srs.models.Registry;

public class RegistryNotDeletedException extends Exception {

    public RegistryNotDeletedException(Registry r, Exception cause) {
        super("registry instance is not deleted\n" + r.toString(), cause);
    }
}
