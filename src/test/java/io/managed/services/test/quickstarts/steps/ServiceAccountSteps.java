package io.managed.services.test.quickstarts.steps;

import com.openshift.cloud.api.kas.models.ServiceAccountRequest;
import io.cucumber.java.After;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import io.managed.services.test.Environment;
import io.managed.services.test.client.securitymgmt.SecurityMgmtAPIUtils;
import io.managed.services.test.quickstarts.contexts.OpenShiftAPIContext;
import io.managed.services.test.quickstarts.contexts.ServiceAccountContext;
import lombok.extern.log4j.Log4j2;
import org.testng.SkipException;

import static io.managed.services.test.TestUtils.assumeTeardown;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

@Log4j2
public class ServiceAccountSteps {

    private final OpenShiftAPIContext openShiftAPIContext;
    private final ServiceAccountContext serviceAccountContext;

    private static final String SERVICE_ACCOUNT_UNIQUE_NAME = "cucumber-sa-qs-"  + Environment.LAUNCH_SUFFIX;

    public ServiceAccountSteps(OpenShiftAPIContext openShiftAPIContext, ServiceAccountContext serviceAccountContext) {
        this.openShiftAPIContext = openShiftAPIContext;
        this.serviceAccountContext = serviceAccountContext;
    }

    @When("you create a service account with a unique name")
    public void you_create_a_service_account_with_a_unique_name() throws Throwable {
        var securityMgmtApi = openShiftAPIContext.requireSecurityMgmtApi();

        log.info("create service account with name '{}'", SERVICE_ACCOUNT_UNIQUE_NAME);
        var payload = new ServiceAccountRequest();
        payload.setName(SERVICE_ACCOUNT_UNIQUE_NAME);
        var serviceAccount = securityMgmtApi.createServiceAccount(payload);
        log.debug(serviceAccount);

        serviceAccountContext.setServiceAccount(serviceAccount);
    }

    @Then("the service account has a generated Client ID and Client Secret")
    public void the_service_account_has_a_generated_client_id_and_client_secret() {
        var serviceAccount = serviceAccountContext.requireServiceAccount();

        assertNotNull(serviceAccount.getClientId());
        assertNotNull(serviceAccount.getClientSecret());
    }

    @Then("the service account is listed in the service accounts table")
    public void the_service_account_is_listed_in_the_service_accounts_table() throws Throwable {
        var securityMgmtApi = openShiftAPIContext.requireSecurityMgmtApi();

        var list = securityMgmtApi.getServiceAccounts();
        log.debug(list);

        var o = list.getItems().stream()
            .filter(a -> SERVICE_ACCOUNT_UNIQUE_NAME.equals(a.getName()))
            .findAny();
        assertTrue(o.isPresent());
    }

    @Given("you have the generated credentials for your service account")
    public void you_have_the_generated_credentials_for_your_service_account() throws Throwable {
        var securityMgmtApi = openShiftAPIContext.requireSecurityMgmtApi();

        log.info("load service account with name '{}'", SERVICE_ACCOUNT_UNIQUE_NAME);
        var serviceAccount = SecurityMgmtAPIUtils.applyServiceAccount(securityMgmtApi, SERVICE_ACCOUNT_UNIQUE_NAME);
        log.debug(serviceAccount);

        serviceAccountContext.setServiceAccount(serviceAccount);
    }

    @After
    public void teardown() {
        assumeTeardown();

        var securityMgmtApi = openShiftAPIContext.getSecurityMgmtApi();
        if (securityMgmtApi == null) {
            throw new SkipException("skip service account teardown");
        }

        // delete service account
        try {
            log.info("clean service account '{}'", SERVICE_ACCOUNT_UNIQUE_NAME);
            SecurityMgmtAPIUtils.cleanServiceAccount(securityMgmtApi, SERVICE_ACCOUNT_UNIQUE_NAME);
        } catch (Throwable t) {
            log.error("clean service account error: ", t);
        }

        serviceAccountContext.setServiceAccount(null);
    }
}
