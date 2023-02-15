package io.managed.services.test.client.registrymgmt;

import com.openshift.cloud.api.srs.ApiClient;
import com.openshift.cloud.api.srs.api.serviceregistry_mgmt.v1.V1RequestBuilder;
import com.openshift.cloud.api.srs.models.RegistryCreate;
import com.openshift.cloud.api.srs.models.RegistryList;
import com.openshift.cloud.api.srs.models.RootTypeForRegistry;
import io.managed.services.test.client.BaseApi;
import io.managed.services.test.client.exception.ApiGenericException;
import io.managed.services.test.client.exception.ApiUnknownException;

import java.util.concurrent.TimeUnit;

public class RegistryMgmtApi extends BaseApi {

    private final ApiClient apiClient;
    private final V1RequestBuilder v1;

    public RegistryMgmtApi(ApiClient apiClient, String offlineToken) {
        super(offlineToken);
        this.apiClient = apiClient;
        this.v1 = apiClient.api().serviceregistry_mgmt().v1();
    }

    @Override
    protected ApiUnknownException toApiException(Exception e) {
        return null;
    }

    public RootTypeForRegistry createRegistry(RegistryCreate registryCreateRest) throws ApiGenericException {
        return retry(() -> v1.registries().post(registryCreateRest).get(10, TimeUnit.SECONDS));
    }

    public RootTypeForRegistry getRegistry(String id) throws ApiGenericException {
        return retry(() -> v1.registries(id).get().get(10, TimeUnit.SECONDS));
    }

    public RegistryList getRegistries(Integer page, Integer size, String orderBy, String search) throws ApiGenericException {
        return retry(() -> v1.registries().get(config -> {
            config.queryParameters.page = page;
            config.queryParameters.size = size;
            config.queryParameters.orderBy = orderBy;
            config.queryParameters.search = search;
        }).get(10, TimeUnit.SECONDS));
    }

    public void deleteRegistry(String id) throws ApiGenericException {
        retry(() -> v1.registries(id).delete().get(10, TimeUnit.SECONDS));
    }
}
