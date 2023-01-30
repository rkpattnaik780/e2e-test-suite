package io.managed.services.test.client.securitymgmt;

import com.openshift.cloud.api.kas.SecurityApi;
import com.openshift.cloud.api.kas.invoker.ApiClient;
import com.openshift.cloud.api.kas.invoker.ApiException;
import com.openshift.cloud.api.kas.invoker.auth.HttpBearerAuth;
import com.openshift.cloud.api.kas.models.ServiceAccount;
import com.openshift.cloud.api.kas.models.ServiceAccountList;
import com.openshift.cloud.api.kas.models.ServiceAccountRequest;
import io.managed.services.test.client.BaseApi;
import io.managed.services.test.client.exception.ApiGenericException;
import io.managed.services.test.client.exception.ApiUnknownException;

public class SecurityMgmtApi extends BaseApi {

    private final ApiClient apiClient;
    private final SecurityApi api;

    public SecurityMgmtApi(ApiClient apiClient, String offlineToken) {
        super(offlineToken);
        this.apiClient = apiClient;
        this.api = new SecurityApi(apiClient);
    }

    @Override
    protected ApiUnknownException toApiException(Exception e) {
        if (e instanceof ApiException) {
            var ex = (ApiException) e;
            return new ApiUnknownException(ex.getMessage(), ex.getCode(), ex.getResponseHeaders(), ex.getResponseBody(), ex);
        }
        return null;
    }

    @Override
    protected void setAccessToken(String accessToken) {
        ((HttpBearerAuth) apiClient.getAuthentication("Bearer")).setBearerToken(accessToken);
    }

    @SuppressWarnings("unused")
    public ServiceAccount getServiceAccountById(String id) throws ApiGenericException {
        return retry(() -> api.getServiceAccountById(id));
    }

    public ServiceAccountList getServiceAccounts() throws ApiGenericException {
        return retry(() -> api.getServiceAccounts(null));
    }

    public ServiceAccount createServiceAccount(ServiceAccountRequest serviceAccountRequest) throws ApiGenericException {
        return retry(() -> api.createServiceAccount(serviceAccountRequest));
    }

    public void deleteServiceAccountById(String id) throws ApiGenericException {
        // TODO: why does it return Error
        retry(() -> api.deleteServiceAccountById(id));
    }

    public ServiceAccount resetServiceAccountCreds(String id) throws ApiGenericException {
        return retry(() -> api.resetServiceAccountCreds(id));
    }
}
