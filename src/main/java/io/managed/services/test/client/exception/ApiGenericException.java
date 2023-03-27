package io.managed.services.test.client.exception;

import java.net.HttpURLConnection;

public class ApiGenericException extends Exception {

    private final String code;
    private final String href;
    private final String id;

    private final String reason;
    private final int responseStatusCode;

    public final static String API_ERROR_BILLING_ACCOUNT_INVALID = "43";
    public final static String API_ERROR_INSUFFICIENT_QUOTA = "120";


    public ApiGenericException(ApiUnknownException e) {
        super(e.getFullMessage(), e);
        this.responseStatusCode = e.getResponseStatusCode();
        this.reason = e.getReason();
        this.code = e.getCode();
        this.href = e.getHref();
        this.id = e.getId();
    }

    public ApiGenericException(ApiGenericException e) {
        super(e.getReason(), e);
        this.responseStatusCode = e.getResponseStatusCode();
        this.reason = e.getReason();
        this.code = e.getCode();
        this.href = e.getHref();
        this.id = e.getId();
    }

    public ApiGenericException(
            String reason,
            String code,
            int responseStatusCode,
            String href,
            String id,
            Exception cause) {

        super(reason, cause);
        this.reason = reason;
        this.code = code;
        this.responseStatusCode = responseStatusCode;
        this.href = href;
        this.id = id;
    }

    public int getResponseStatusCode() {
        return responseStatusCode;
    }

    public String getReason() {
        return reason;
    }

    public String getCode() {
        return code;
    }

    public String getHref() {
        return href;
    }

    public String getId() {
        return id;
    }

    public static ApiGenericException apiException(ApiGenericException e) {
        switch (e.getResponseStatusCode()) {
            case HttpURLConnection.HTTP_NOT_FOUND:
                return new ApiNotFoundException(e);
            case HttpURLConnection.HTTP_UNAUTHORIZED:
                return new ApiUnauthorizedException(e);
            case HttpURLConnection.HTTP_FORBIDDEN:
                return new ApiForbiddenException(e);
            case 429:
                return new ApiToManyRequestsException(e);
            case HttpURLConnection.HTTP_CONFLICT:
                return new ApiConflictException(e);
            case 423:
                return new ApiLockedException(e);
            default:
                return new ApiGenericException(e);
        }
    }
}
