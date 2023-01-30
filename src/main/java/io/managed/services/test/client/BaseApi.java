package io.managed.services.test.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.managed.services.test.RetryUtils;
import io.managed.services.test.ThrowingSupplier;
import io.managed.services.test.ThrowingVoid;
import io.managed.services.test.client.exception.ApiGenericException;
import io.managed.services.test.client.exception.ApiUnknownException;
import lombok.extern.log4j.Log4j2;
import okhttp3.FormBody;
import okhttp3.OkHttpClient;
import okhttp3.Request;

import java.util.concurrent.atomic.AtomicReference;

@Log4j2
public abstract class BaseApi {

    public final static String RH_SSO_URL = "https://sso.redhat.com/auth/realms/redhat-external/protocol/openid-connect/token";


    private final String offlineToken;
    private final String url;

    protected BaseApi(String offlineToken) {
        this.offlineToken = offlineToken;
        this.url = RH_SSO_URL;
    }

    protected BaseApi(String offlineToken, String url) {
        this.offlineToken = offlineToken;
        this.url = url;
    }

    /**
     * @param e Exception
     * @return ApiUnknownException | null if the passed Exception can't be converted
     */
    protected abstract ApiUnknownException toApiException(Exception e);

    protected abstract void setAccessToken(String t);

    private <A> A handleException(ThrowingSupplier<A, Exception> f) throws ApiGenericException {

        try {
            return f.get();
        } catch (Exception e) {
            var ex = toApiException(e);
            if (ex != null) {
                throw ApiGenericException.apiException(ex);
            }
            throw new RuntimeException(e);
        }
    }

    private final OkHttpClient client = new OkHttpClient();
    private final ObjectMapper mapper = new ObjectMapper();
    private final AtomicReference<String> lastRefreshToken = new AtomicReference<>();

    private String newToken() {
        String currentToken = lastRefreshToken.get();
        if (currentToken == null) {
            var data = new FormBody.Builder()
                    .add("grant_type", "refresh_token")
                    .add("client_id", "rhsm-api")
                    .add("refresh_token", this.offlineToken)
                    .build();

            var request = new Request.Builder()
                    .url(url)
                    .post(data)
                    .build();

            String accessToken = null;
            try {
                var response = client.newCall(request).execute();
                accessToken = mapper.readTree(response.body().string()).get("access_token").asText();
            } catch (Exception e) {
                throw new RuntimeException("Failed to get credentials", e);
            }

            lastRefreshToken.set(accessToken);
            return accessToken;
        } else {
            return currentToken;
        }
    }

    private <A> A handle(ThrowingSupplier<A, Exception> f) throws ApiGenericException {
        // Set the access token before each call because another API could
        // have renewed it
        setAccessToken(newToken());

        try {
            return handleException(f);
        } catch (RuntimeException e) {
            log.debug("renew access token");
            // Try to renew the access token
            lastRefreshToken.set(null);
            setAccessToken(newToken());
            // and retry
            return handleException(f);
        }
    }

    protected <A> A retry(ThrowingSupplier<A, Exception> f) throws ApiGenericException {
        return RetryUtils.retry(1, () -> handle(f), BaseApi::retryCondition);
    }

    protected void retry(ThrowingVoid<Exception> f) throws ApiGenericException {
        RetryUtils.retry(1, () -> handle(f.toSupplier()), BaseApi::retryCondition);
    }

    private static boolean retryCondition(Throwable t) {
        if (t instanceof ApiGenericException) {
            var code = ((ApiGenericException) t).getCode();
            return code >= 500 && code < 600 // Server Errors
                || code == 408;  // Request Timeout
        }
        if (t instanceof RuntimeException) {
            // retry generic runtime exception
            return true;
        }
        log.warn("not going to retry exception:", t);
        return false;
    }
}
