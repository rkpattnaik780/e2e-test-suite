package io.managed.services.test.client.exception;

public class ApiLockedException extends ApiGenericException {
    public ApiLockedException(ApiGenericException e) {
        super(e);
    }
}
