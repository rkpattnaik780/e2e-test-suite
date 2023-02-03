package io.managed.services.test.quickstarts.steps;

import io.cucumber.java.en.Given;
import io.managed.services.test.Environment;
import io.managed.services.test.client.kafkamgmt.KafkaMgmtApiUtils;
import io.managed.services.test.client.securitymgmt.SecurityMgmtAPIUtils;
import io.managed.services.test.quickstarts.contexts.OpenShiftAPIContext;
import lombok.extern.log4j.Log4j2;
import static org.testng.Assert.assertNotNull;

@Log4j2
public class LoginSteps {

    private final OpenShiftAPIContext openShiftAPI;

    public LoginSteps(OpenShiftAPIContext openShiftAPI) {
        this.openShiftAPI = openShiftAPI;
    }

    @Given("you have a Red Hat account")
    public void you_have_a_red_hat_account() {
        assertNotNull(Environment.PRIMARY_OFFLINE_TOKEN, "the PRIMARY_OFFLINE_TOKEN env is null");
    }

    @Given("you are logged in to the OpenShift Streams for Apache Kafka")
    public void you_are_logged_in_to_the_open_shift_streams_for_apache_kafka() throws Throwable {

        // initialize APIs
        log.info("initialize KafkaMgmtAPI with base path '{}'", Environment.OPENSHIFT_API_URI);
        var kafkaMgmtApi = KafkaMgmtApiUtils.kafkaMgmtApi(Environment.OPENSHIFT_API_URI, Environment.PRIMARY_OFFLINE_TOKEN);

        log.info("initialize SecurityMgmtAPI with base path '{}'", Environment.OPENSHIFT_API_URI);
        var securityMgmtApi = SecurityMgmtAPIUtils.securityMgmtApi(Environment.OPENSHIFT_API_URI, Environment.PRIMARY_OFFLINE_TOKEN);

        // update contexts
        openShiftAPI.setKafkaMgmtApi(kafkaMgmtApi);
        openShiftAPI.setSecurityMgmtApi(securityMgmtApi);

    }
}
