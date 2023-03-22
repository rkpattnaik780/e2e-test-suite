package io.managed.services.test.client.registry;
import com.openshift.cloud.api.registry.instance.models.RoleMapping;
import com.openshift.cloud.api.registry.instance.models.ArtifactMetaData;
import com.openshift.cloud.api.registry.instance.models.ContentCreateRequest;
import io.managed.services.test.client.BaseApi;
import io.managed.services.test.client.exception.ApiGenericException;
import com.openshift.cloud.api.registry.instance.ApiClient;
import java.util.concurrent.TimeUnit;

public class RegistryClient extends BaseApi {

    private final ApiClient apiClient;

    public RegistryClient(ApiClient apiClient) {
        super();
        this.apiClient = apiClient;
    }

    @Override
    protected ApiGenericException toApiException(Exception e) {
        if (e.getCause() instanceof com.microsoft.kiota.ApiException) {
            var err = (com.microsoft.kiota.ApiException) e.getCause();
            return new ApiGenericException(err.getMessage(), "", err.responseStatusCode, "", "", err);
        }

        return null;
    }

    public ArtifactMetaData createArtifact(ContentCreateRequest data) throws ApiGenericException {
        String groupIdd = "default";

        return retry(() -> apiClient.groups(groupIdd).artifacts().post(data, config -> config.headers.add("X-Registry-ArtifactType", "JSON")).get(5, TimeUnit.SECONDS));
    }

    public void createRoleMapping(RoleMapping data) throws ApiGenericException {
        retry(() -> apiClient.admin().roleMappings().post(data));
    }
}
