package io.managed.services.test.client.registry;

import com.microsoft.kiota.authentication.BaseBearerTokenAuthenticationProvider;
import com.microsoft.kiota.http.OkHttpRequestAdapter;
import com.openshift.cloud.api.registry.instance.ApiClient;
import com.redhat.cloud.kiota.auth.RHAccessTokenProvider;

public class RegistryClientUtils {
//    private static final Logger LOGGER = LogManager.getLogger(RegistryClientUtils.class);

//    public static RegistryClientApi registryClient(String uri, String offlineToken) {
//        return new RegistryClientApi(uri, offlineToken);
//    }

    public static RegistryClient registryClient2(String uri, String offlineToken) {

        var adapter = new OkHttpRequestAdapter(new BaseBearerTokenAuthenticationProvider(new RHAccessTokenProvider(offlineToken)));
        adapter.setBaseUrl(uri + "/apis/registry/v2");
        ApiClient client = new ApiClient(adapter);

        return new RegistryClient(client);
    }
}
