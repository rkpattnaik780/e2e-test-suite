package io.managed.services.test.client.exception;

public class ApiConflictException extends ApiGenericException {
    public ApiConflictException(ApiGenericException e) {
        super(e);
    }
}
