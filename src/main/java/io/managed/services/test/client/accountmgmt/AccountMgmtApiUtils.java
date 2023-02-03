package io.managed.services.test.client.accountmgmt;

import io.managed.services.test.Environment;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

public class AccountMgmtApiUtils {

    public static String getPrimaryUserUsername() throws ExecutionException, InterruptedException, TimeoutException {
        return  new AccountMgmtApi(Environment.OPENSHIFT_API_URI, Environment.PRIMARY_OFFLINE_TOKEN).getAccountUsername();
    }

    public static String getSecondaryUserUsername() throws ExecutionException, InterruptedException, TimeoutException {
        return  new AccountMgmtApi(Environment.OPENSHIFT_API_URI, Environment.SECONDARY_OFFLINE_TOKEN).getAccountUsername();
    }

    public static String getAdminUserUsername() throws ExecutionException, InterruptedException, TimeoutException {
        return  new AccountMgmtApi(Environment.OPENSHIFT_API_URI, Environment.ADMIN_OFFLINE_TOKEN).getAccountUsername();
    }
}
