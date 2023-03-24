package io.managed.services.test.client.exception;

public class ApiUnauthorizedException extends ApiGenericException {
    public ApiUnauthorizedException(ApiGenericException e) {
        super(e);
    }
}
