package io.managed.services.test.client.accountmgmt;

import com.microsoft.kiota.authentication.BaseBearerTokenAuthenticationProvider;
import com.microsoft.kiota.http.OkHttpRequestAdapter;
import com.openshift.cloud.api.accountmanagement.ApiClient;
import com.openshift.cloud.api.accountmanagement.models.Account;
import com.redhat.cloud.kiota.auth.RHAccessTokenProvider;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class AccountMgmtApi {

    private final ApiClient accountManagementClient;

    public AccountMgmtApi(String basePath, String offlineToken) {

        //initiate API.
        var requestAdapter = new OkHttpRequestAdapter(new BaseBearerTokenAuthenticationProvider(new RHAccessTokenProvider(offlineToken)));
        requestAdapter.setBaseUrl(basePath);
        accountManagementClient = new ApiClient(requestAdapter);
    }

    public String getAccountUsername() throws ExecutionException, InterruptedException, TimeoutException {
        // TODO experimental implementation will be changed with better responses from provided SDK, with own exception type
        Account currentAccount = accountManagementClient.api().accounts_mgmt().v1().current_account().get().get(5, TimeUnit.SECONDS);
        return  currentAccount.getUsername();
    }

}
