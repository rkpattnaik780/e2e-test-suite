package io.managed.services.test.client.securitymgmt;

import com.openshift.cloud.api.kas.ApiClient;
import com.openshift.cloud.api.kas.api.kafkas_mgmt.v1.V1RequestBuilder;
import com.openshift.cloud.api.kas.api.kafkas_mgmt.v1.service_accounts.ServiceAccountListResponse;
import com.openshift.cloud.api.kas.models.ServiceAccount;
import com.openshift.cloud.api.kas.models.ServiceAccountRequest;
import io.managed.services.test.client.BaseApi;
import io.managed.services.test.client.exception.ApiGenericException;
import lombok.extern.log4j.Log4j2;

import java.util.concurrent.TimeUnit;

@Log4j2
public class SecurityMgmtApi extends BaseApi {

    private final V1RequestBuilder v1;

    public SecurityMgmtApi(ApiClient apiClient) {
        super();
        this.v1 = apiClient.api().kafkas_mgmt().v1();
    }

    @SuppressWarnings("unused")
    public ServiceAccount getServiceAccountById(String id) throws ApiGenericException {
        return retry(() -> v1.service_accounts(id).get().get(10, TimeUnit.SECONDS));
    }

    public ServiceAccountListResponse getServiceAccounts() throws ApiGenericException {
        return retry(() -> v1.service_accounts().get().get(10, TimeUnit.SECONDS));
    }

    public ServiceAccount createServiceAccount(ServiceAccountRequest serviceAccountRequest) throws ApiGenericException {
        return retry(() -> v1.service_accounts().post(serviceAccountRequest).get(10, TimeUnit.SECONDS));
    }

    public void deleteServiceAccountById(String id) throws ApiGenericException {
        // TODO: why does it return Error
        retry(() -> v1.service_accounts(id).delete().get(10, TimeUnit.SECONDS));
    }

    public ServiceAccount resetServiceAccountCreds(String id) throws ApiGenericException {
        return retry(() -> v1.service_accounts(id).reset_credentials().post().get(10, TimeUnit.SECONDS));
    }

    @Override
    protected ApiGenericException toApiException(Exception e) {

        if (e.getCause() != null) {
            log.info(e);
            if (e.getCause() instanceof com.microsoft.kiota.ApiException) {
                var err = (com.microsoft.kiota.ApiException) e.getCause();
                return new ApiGenericException(err.getMessage(), "", err.responseStatusCode, "", "", err);
            }
        }

        return null;
    }
}
