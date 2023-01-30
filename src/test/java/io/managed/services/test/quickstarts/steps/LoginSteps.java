package io.managed.services.test.quickstarts.steps;

import io.cucumber.java.en.Given;
import io.managed.services.test.Environment;
import io.managed.services.test.quickstarts.contexts.OpenShiftAPIContext;
import io.managed.services.test.quickstarts.contexts.UserContext;
import lombok.extern.log4j.Log4j2;

import static org.testng.Assert.assertNotNull;

@Log4j2
public class LoginSteps {

    private final UserContext user;
    private final OpenShiftAPIContext openShiftAPI;

    public LoginSteps(UserContext user, OpenShiftAPIContext openShiftAPI) {
        this.user = user;
        this.openShiftAPI = openShiftAPI;
    }

    @Given("you have a Red Hat account")
    public void you_have_a_red_hat_account() {
        assertNotNull(Environment.PRIMARY_OFFLINE_TOKEN, "the PRIMARY_OFFLINE_TOKEN env is null");
    }
}
