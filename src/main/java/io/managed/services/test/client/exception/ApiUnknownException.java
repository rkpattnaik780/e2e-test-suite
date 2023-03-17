package io.managed.services.test.client.exception;

public class ApiUnknownException extends Exception {

    private final String code;
    private final String href;
    private final String id;

    private final String reason;
    private final int responseStatusCode;


    public ApiUnknownException(
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

    public String getCode() {
        return code;
    }
    public String getHref() {
        return href;
    }
    public String getId() {
        return id;
    }
    public int getResponseStatusCode() {
        return responseStatusCode;
    }
    public String getReason() {
        return reason;
    }

    public String getFullMessage() {
        var error = new StringBuilder();
        error.append(getMessage());
        error.append(String.format("\nCode: %s", getCode()));
        error.append(String.format("\nHref: %s", getHref()));
        error.append(String.format("\nResponse Status Code: %s", getResponseStatusCode()));
        error.append(String.format("\nReason (Msg): %s", getReason()));
        return error.toString();
    }
}
