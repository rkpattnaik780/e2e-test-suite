package io.managed.services.test.client.exception;

public class ApiNotFoundException extends ApiGenericException {
    public ApiNotFoundException(ApiGenericException e) {
        super(e);
    }
}
