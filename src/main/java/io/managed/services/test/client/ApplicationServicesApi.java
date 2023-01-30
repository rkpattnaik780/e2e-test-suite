package io.managed.services.test.client;

import io.managed.services.test.Environment;
import io.managed.services.test.client.kafkamgmt.KafkaMgmtApi;
import io.managed.services.test.client.kafkamgmt.KafkaMgmtApiUtils;
import io.managed.services.test.client.registrymgmt.RegistryMgmtApi;
import io.managed.services.test.client.registrymgmt.RegistryMgmtApiUtils;
import io.managed.services.test.client.securitymgmt.SecurityMgmtAPIUtils;
import io.managed.services.test.client.securitymgmt.SecurityMgmtApi;
import lombok.extern.log4j.Log4j2;

@Log4j2
@Deprecated
public class ApplicationServicesApi {

    private final KafkaMgmtApi kafkaMgmtApi;
    private final SecurityMgmtApi securityMgmtApi;
    private final RegistryMgmtApi registryMgmtApi;

    public ApplicationServicesApi(String basePath, String offlineToken) {
        this.kafkaMgmtApi = KafkaMgmtApiUtils.kafkaMgmtApi(basePath, offlineToken);
        this.securityMgmtApi = SecurityMgmtAPIUtils.securityMgmtApi(basePath, offlineToken);
        this.registryMgmtApi = RegistryMgmtApiUtils.registryMgmtApi(basePath, offlineToken);
    }

    public static ApplicationServicesApi applicationServicesApi(String offlineToken) {
        return new ApplicationServicesApi(Environment.OPENSHIFT_API_URI, offlineToken);
    }

    public KafkaMgmtApi kafkaMgmt() {
        return kafkaMgmtApi;
    }

    public SecurityMgmtApi securityMgmt() {
        return securityMgmtApi;
    }

    public RegistryMgmtApi registryMgmt() {
        return registryMgmtApi;
    }
}
