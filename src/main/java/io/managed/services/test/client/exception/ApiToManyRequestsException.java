package io.managed.services.test.client.exception;

public class ApiToManyRequestsException extends ApiGenericException {
    public ApiToManyRequestsException(ApiGenericException e) {
        super(e);
    }
}
