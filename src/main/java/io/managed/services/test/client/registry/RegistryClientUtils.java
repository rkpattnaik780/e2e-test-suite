package io.managed.services.test.client.registry;

import com.github.andreatp.kiota.auth.RHAccessTokenProvider;
import com.microsoft.kiota.authentication.BaseBearerTokenAuthenticationProvider;
import com.microsoft.kiota.http.OkHttpRequestAdapter;
import com.openshift.cloud.api.registry.instance.ApiClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class RegistryClientUtils {
    private static final Logger LOGGER = LogManager.getLogger(RegistryClientUtils.class);

    public static RegistryClientApi registryClient(String uri, String offlineToken) {
        return new RegistryClientApi(uri, offlineToken);
    }

    public static RegistryClient2 registryClient2(String uri, String offlineToken) {

        var adapter = new OkHttpRequestAdapter(new BaseBearerTokenAuthenticationProvider(new RHAccessTokenProvider(offlineToken)));
        adapter.setBaseUrl(uri);
        ApiClient client = new ApiClient(adapter);

        return new RegistryClient2(client, offlineToken);
    }
}
