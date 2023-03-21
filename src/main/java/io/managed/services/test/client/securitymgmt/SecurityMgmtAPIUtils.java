package io.managed.services.test.client.securitymgmt;


import com.microsoft.kiota.authentication.BaseBearerTokenAuthenticationProvider;
import com.microsoft.kiota.http.OkHttpRequestAdapter;
import com.openshift.cloud.api.kas.ApiClient;
import com.openshift.cloud.api.kas.models.ServiceAccount;
import com.openshift.cloud.api.kas.models.ServiceAccountListItem;
import com.openshift.cloud.api.kas.models.ServiceAccountRequest;
import com.redhat.cloud.kiota.auth.RHAccessTokenProvider;
import io.managed.services.test.client.exception.ApiGenericException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Optional;
import java.util.stream.Collectors;


public class SecurityMgmtAPIUtils {
    private static final Logger LOGGER = LogManager.getLogger(SecurityMgmtAPIUtils.class);

    public static SecurityMgmtApi securityMgmtApi(String uri, String offlineToken) {
        var adapter = new OkHttpRequestAdapter(new BaseBearerTokenAuthenticationProvider(new RHAccessTokenProvider(offlineToken)));
        adapter.setBaseUrl(uri);
        return new SecurityMgmtApi(new ApiClient(adapter));
    }

    /**
     * Get Service Account by name or return empty optional
     *
     * @param api  SecurityMgmtApi
     * @param name Service Account name
     * @return Optional ServiceAccount
     */
    public static Optional<ServiceAccountListItem> getServiceAccountByName(SecurityMgmtApi api, String name)
        throws ApiGenericException {

        var list = api.getServiceAccounts();
        return list.getItems().stream().filter(a -> name.equals(a.getName())).findAny();
    }

    /**
     * Delete Service Account by name if it exists
     *
     * @param api  SecurityMgmtApi
     * @param name Service Account name
     */
    @SuppressWarnings("unused")
    public static void deleteServiceAccountByNameIfExists(SecurityMgmtApi api, String name)
        throws ApiGenericException {

        var exists = getServiceAccountByName(api, name);
        if (exists.isPresent()) {
            var serviceAccount = exists.get();
            LOGGER.info("delete service account '{}'", serviceAccount.getName());
            LOGGER.debug(serviceAccount);
            api.deleteServiceAccountById(serviceAccount.getId());
            LOGGER.info("service account '{}' deleted", serviceAccount.getName());
        } else {
            LOGGER.info("service account '{}' not found", name);
        }
    }

    /**
     * If the service account with the passed name doesn't exist, recreate it, otherwise reset the credentials
     * and return the ServiceAccount with clientSecret
     *
     * @param api  SecurityMgmtApi
     * @param name Service Account Name
     * @return ServiceAccount with clientSecret
     */
    public static ServiceAccount applyServiceAccount(SecurityMgmtApi api, String name)
        throws ApiGenericException {

        var existing = getServiceAccountByName(api, name);

        ServiceAccount serviceAccount;
        if (existing.isPresent()) {
            LOGGER.warn("reset service account '{}' credentials", existing.get().getName());
            serviceAccount = api.resetServiceAccountCreds(existing.get().getId());
            LOGGER.debug(serviceAccount);
        } else {
            LOGGER.info("create service account '{}'", name);
            var req = new ServiceAccountRequest();
            req.setName(name);
            req.setDescription("E2E test service account");
            serviceAccount = api.createServiceAccount(req);
        }
        return serviceAccount;
    }

    /**
     * Because at the time I wrote this the service account name is not unique we need to delete
     * all service accounts with the same name.
     *
     * @param api  {@link SecurityMgmtApi}
     * @param name Name of the service account to delete
     */
    public static void cleanServiceAccount(SecurityMgmtApi api, String name) throws ApiGenericException {

        var accounts = api.getServiceAccounts().getItems()
            .stream().filter(a -> name.equals(a.getName()))
            .collect(Collectors.toList());

        for (var a : accounts) {
            try {
                api.deleteServiceAccountById(a.getId());
            } catch (ApiGenericException e) {
                LOGGER.error("failed to delete service account with id '{}':", a.getId(), e);
            }
        }
    }
}
