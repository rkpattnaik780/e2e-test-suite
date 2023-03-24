package io.managed.services.test.client.exception;

public class ApiForbiddenException extends ApiGenericException {
    public ApiForbiddenException(ApiGenericException e) {
        super(e);
    }
}
