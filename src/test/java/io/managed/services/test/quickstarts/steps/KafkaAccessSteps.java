package io.managed.services.test.quickstarts.steps;

import com.openshift.cloud.api.kas.auth.models.AclBinding;
import com.openshift.cloud.api.kas.auth.models.AclOperation;
import com.openshift.cloud.api.kas.auth.models.AclPatternType;
import com.openshift.cloud.api.kas.auth.models.AclPermissionType;
import com.openshift.cloud.api.kas.auth.models.AclResourceType;
import io.cucumber.java.After;
import io.cucumber.java.en.Given;
import io.managed.services.test.client.kafkainstance.KafkaInstanceApiAccessUtils;
import io.managed.services.test.quickstarts.contexts.KafkaInstanceContext;
import io.managed.services.test.quickstarts.contexts.ServiceAccountContext;
import lombok.extern.log4j.Log4j2;

import static io.managed.services.test.TestUtils.assumeTeardown;

@Log4j2
public class KafkaAccessSteps {

    private final KafkaInstanceContext kafkaInstanceContext;
    private final ServiceAccountContext serviceAccountContext;

    // If access is created for this principal it will be clean in the teardown
    private String principal;

    public KafkaAccessSteps(KafkaInstanceContext kafkaInstanceContext, ServiceAccountContext serviceAccountContext) {
        this.kafkaInstanceContext = kafkaInstanceContext;
        this.serviceAccountContext = serviceAccountContext;
    }

    @Given("you have set the permissions for your service account to access your Kafka instance resources")
    public void you_have_set_the_permissions_for_your_service_account_to_access_your_kafka_instance_resources() throws Throwable {
        var kafkaInstanceApi = kafkaInstanceContext.kafkaInstanceApi();
        var serviceAccount = serviceAccountContext.requireServiceAccount();

        var principal = KafkaInstanceApiAccessUtils.toPrincipal(serviceAccount.getClientId());
        log.info("apply producer and consumer ACLs for principal '{}'", principal);
        KafkaInstanceApiAccessUtils.applyProducerAndConsumerACLs(kafkaInstanceApi, principal);

        this.principal = principal;
    }

    @Given("you have set the permissions for your service account to manipulate topic")
    public void you_have_set_the_permissions_for_your_service_account_to_manipulate_topic() throws Throwable {
        var kafkaInstanceApi = kafkaInstanceContext.kafkaInstanceApi();
        var serviceAccount = serviceAccountContext.requireServiceAccount();

        var principal = KafkaInstanceApiAccessUtils.toPrincipal(serviceAccount.getClientId());
        log.info("apply acl to create topics for principal '{}'", principal);

        var acl = new AclBinding();
        acl.setPrincipal(principal);
        acl.setResourceType(AclResourceType.TOPIC);
        acl.setPatternType(AclPatternType.LITERAL);
        acl.setResourceName("*");
        acl.setPermission(AclPermissionType.ALLOW);
        acl.setOperation(AclOperation.ALL);

        log.debug(acl);

        kafkaInstanceApi.createAcl(acl);

        this.principal = principal;
    }

    @After(order = 10100)
    public void teardown() {
        assumeTeardown();

        if (principal != null) {
            try {
                var kafkaMgmtApi = kafkaInstanceContext.kafkaInstanceApi();
                log.info("clean acls for principal '{}'", principal);
                kafkaMgmtApi.deleteAcls(null, null, null, principal, null, null);
            } catch (Throwable t) {
                log.error("failed to clean access:", t);
            }

            principal = null;
        }
    }
}
