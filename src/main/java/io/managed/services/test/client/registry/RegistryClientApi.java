package io.managed.services.test.client.registry;

import io.apicurio.registry.rest.client.RegistryClient;
import io.apicurio.registry.rest.client.RegistryClientFactory;
import io.apicurio.registry.rest.client.exception.RateLimitedClientException;
import io.apicurio.registry.rest.client.exception.RestClientException;
import io.apicurio.registry.rest.v2.beans.ArtifactMetaData;
import io.apicurio.registry.rest.v2.beans.RoleMapping;
import io.apicurio.rest.client.auth.exception.ForbiddenException;
import io.apicurio.rest.client.auth.exception.NotAuthorizedException;
import io.managed.services.test.client.BaseApi;
import io.managed.services.test.client.exception.ApiGenericException;
import io.managed.services.test.client.exception.ApiUnknownException;

import java.io.ByteArrayInputStream;
import java.net.HttpURLConnection;
import java.util.HashMap;

public class RegistryClientApi extends BaseApi {

    private final RegistryClient registryClient;
    private final BearerAuth bearerAuth;

    public RegistryClientApi(String baseUrl, String offlineToken) {
        super(offlineToken);
        this.bearerAuth = new BearerAuth();
        this.registryClient = RegistryClientFactory.create(baseUrl, new HashMap<>(), bearerAuth);
    }

    @Override
    protected ApiUnknownException toApiException(Exception e) {
        if (e instanceof NotAuthorizedException) {
            return new ApiUnknownException(e.getMessage(), HttpURLConnection.HTTP_UNAUTHORIZED, new HashMap<>(), "", e);
        }
        if (e instanceof ForbiddenException) {
            return new ApiUnknownException(e.getMessage(), HttpURLConnection.HTTP_FORBIDDEN, new HashMap<>(), "", e);
        }
        if (e instanceof RateLimitedClientException) {
            return new ApiUnknownException(e.getMessage(), 429, new HashMap<>(), "", e);
        }
        if (e instanceof RestClientException) {
            return new ApiUnknownException(e.getMessage(), ((RestClientException) e).getError().getErrorCode(), new HashMap<>(), "", e);
        }
        return null;
    }

    public ArtifactMetaData createArtifact(String groupId, String artifactId, byte[] data) throws ApiGenericException {
        return retry(() -> registryClient.createArtifact(groupId, artifactId, new ByteArrayInputStream(data)));
    }

    public void createRoleMapping(RoleMapping data) throws ApiGenericException {
        retry(() -> registryClient.createRoleMapping(data));
    }
}
