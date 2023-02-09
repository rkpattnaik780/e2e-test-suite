package io.managed.services.test.client.exception;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.net.HttpURLConnection;

public class ApiGenericException extends Exception {

    private final int code;
    private final String responseBody;

    public final static String API_ERROR_BILLING_ACCOUNT_INVALID = "43";
    public final static String API_ERROR_INSUFFICIENT_QUOTA = "120";


    public ApiGenericException(ApiUnknownException e) {
        super(e.getFullMessage(), e);
        this.code = e.getCode();
        this.responseBody = e.getResponseBody();
    }

    public int getCode() {
        return code;
    }

    public String getResponseBody() {
        return responseBody;
    }

    public Body decode() throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(
                DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        return mapper.readValue(getResponseBody(), Body.class);
    }

    public static ApiGenericException apiException(ApiUnknownException e) {
        switch (e.getCode()) {
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

    public static class Body {
        public String reason;
        public String id;
        public String code;
        public String kind;
        public String href;
    }
}
