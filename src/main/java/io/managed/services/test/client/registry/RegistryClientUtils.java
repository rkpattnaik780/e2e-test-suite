package io.managed.services.test.client.registry;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class RegistryClientUtils {
    private static final Logger LOGGER = LogManager.getLogger(RegistryClientUtils.class);

    public static RegistryClientApi registryClient(String uri, String offlineToken) {
        return new RegistryClientApi(uri, offlineToken);
    }
}
