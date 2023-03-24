package io.managed.services.test.client;

import io.managed.services.test.Environment;
import io.managed.services.test.RetryUtils;
import io.managed.services.test.ThrowingSupplier;
import io.managed.services.test.ThrowingVoid;
import io.managed.services.test.client.exception.ApiGenericException;
import lombok.extern.log4j.Log4j2;

@Log4j2
public abstract class BaseApi {

    private String defaultSSOUrl = Environment.REDHAT_SSO_URI + "/auth/realms/" + Environment.REDHAT_SSO_REALM + "/protocol/openid-connect/token";

    private final String url;

    protected BaseApi() {
        this.url = defaultSSOUrl;
    }

    /**
     * @param e Exception
     * @return ApiGenericException | null if the passed Exception can't be converted
     */
    protected abstract ApiGenericException toApiException(Exception e);

    private <A> A handleException(ThrowingSupplier<A, Exception> f) throws ApiGenericException {
        try {
            return f.get();
        } catch (ApiGenericException e) {
            throw e;
        } catch (Exception e) {
            log.info(e);
            var ex = toApiException(e);
            if (ex != null) {
                log.info(ex);
                throw ApiGenericException.apiException(ex);
            }
            throw new RuntimeException(e);
        }
    }

    private <A> A handle(ThrowingSupplier<A, Exception> f) throws ApiGenericException {
        return handleException(f);
    }

    protected <A> A retry(ThrowingSupplier<A, Exception> f) throws ApiGenericException {
        return RetryUtils.retry(1, () -> handle(f), BaseApi::retryCondition);
    }

    protected void retry(ThrowingVoid<Exception> f) throws ApiGenericException {
        RetryUtils.retry(1, () -> handle(f.toSupplier()), BaseApi::retryCondition);
    }

    private static boolean retryCondition(Throwable t) {
        if (t instanceof ApiGenericException) {
            var code = ((ApiGenericException) t).getResponseStatusCode();
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
