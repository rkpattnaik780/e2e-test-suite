package io.managed.services.test.client.kafkainstance;

import com.openshift.cloud.api.kas.auth.models.AclBinding;
import com.openshift.cloud.api.kas.auth.models.AclOperation;
import com.openshift.cloud.api.kas.auth.models.AclPatternType;
import com.openshift.cloud.api.kas.auth.models.AclPermissionType;
import com.openshift.cloud.api.kas.auth.models.AclResourceType;
import com.openshift.cloud.api.kas.models.ServiceAccount;
import io.managed.services.test.client.exception.ApiGenericException;
import lombok.extern.log4j.Log4j2;

import java.util.List;

@Log4j2
public class KafkaInstanceApiAccessUtils {

    /**
     * Reset all ACLs to de desired stated provided by the desiredACLs param.
     *
     * @param api           KafkaInstanceApi
     * @param desiredACLs   The list of desired ACLs that will be created if they don't exist, and any other
     *                      ACLs will be deleted
     */
    public static void resetACLsTo(KafkaInstanceApi api, List<AclBinding> desiredACLs) throws ApiGenericException {
        // get difference of ACLs that are default from ACLs that are currently present
        var currentACLs = getAllACLs(api);

        // create the missing ACLs
        for (var desireACL : desiredACLs) {

            // Remove the desired ACL from the list of current ACLs,
            // if it was removed successfully move on to the next desired ACLs
            // because it means that the desired ACL already exist on the
            // Kafka instance, and that we want to keep it because after
            // checking all desired ACLs the remaining current ACLs will be deleted.
            if (!currentACLs.remove(desireACL)) {
                api.createAcl(desireACL);
            }
        }

        // remove extra ACLs one by one
        for (var currentACL  : currentACLs) {
            deleteACL(api, currentACL);
        }
    }

    public static void deleteACL(KafkaInstanceApi api, AclBinding aclBinding) throws ApiGenericException {

        api.deleteAcls(
            aclBinding.getResourceType(),
            aclBinding.getResourceName(), // TODO: Verify why it was `null`
            aclBinding.getPatternType(),
            aclBinding.getPrincipal(),
            aclBinding.getOperation(),
            aclBinding.getPermission());
    }

    /**
     * Return the current list of ACLs for the Kafka instance.
     *
     * @param api KafkaInstanceApi
     * @return list of current ACLs for the Kafka instance
     */
    public static List<AclBinding> getAllACLs(KafkaInstanceApi api) throws ApiGenericException {
        return api.getAcls(null, null, null, null, null, null, null, null, null, null).getItems();
    }

    /**
     * Convert service account clientID into ACL principal.
     *
     * @param clientID Service account clientID
     * @return The principal name for ACLs
     */
    public static String toPrincipal(String clientID) {
        return "User:" + clientID;
    }

    /**
     * Allow the principal to perform the operation on all resources of the given resource type
     *
     * @param api          KafkaInstanceApi
     * @param principal    The principal id like a service account client id
     * @param resourceType The resource type for which the operation will be allowed
     * @param operation    The operation that will be allowed
     */
    public static void createAllowAnyACL(KafkaInstanceApi api, String principal, AclResourceType resourceType, AclOperation operation)
        throws ApiGenericException {

        var acl = new AclBinding();
        acl.setPrincipal(principal);
        acl.setResourceType(resourceType);
        acl.setPatternType(AclPatternType.LITERAL);
        acl.setResourceName("*");
        acl.setPermission(AclPermissionType.ALLOW);
        acl.setOperation(operation);

        log.debug(acl);

        api.createAcl(acl);
    }

    public static void createReadAnyTopicACL(KafkaInstanceApi api, String principal) throws ApiGenericException {
        log.debug("create read any topic ACL for principal '{}'", principal);
        createAllowAnyACL(api, principal, AclResourceType.TOPIC, AclOperation.READ);
    }

    public static void createWriteAnyTopicACL(KafkaInstanceApi api, String principal) throws ApiGenericException {
        log.debug("create write any topic ACL for principal '{}'", principal);
        createAllowAnyACL(api, principal, AclResourceType.TOPIC, AclOperation.WRITE);
    }

    public static void createReadAnyGroupACL(KafkaInstanceApi api, String principal) throws ApiGenericException {
        log.debug("create read any group ACL for principal '{}'", principal);
        createAllowAnyACL(api, principal, AclResourceType.GROUP, AclOperation.READ);
    }

    /**
     * Allow the principal to consume and produce messages from and to any topics and in any group.
     *
     * @param api       KafkaInstanceApi
     * @param principal The principal id like a service account client id
     */
    public static void createProducerAndConsumerACLs(KafkaInstanceApi api, String principal) throws ApiGenericException {
        createReadAnyGroupACL(api, principal);
        createReadAnyTopicACL(api, principal);
        createWriteAnyTopicACL(api, principal);
    }

    /**
     * Allow the Service account all operations on Topics, Transactions, Groups. In other words grant all ACLs for service account
     *
     * @param api       KafkaInstanceApi
     * @param serviceAccount The serviceAccount instance to be granted permissions
     */
    public static void applyAllowAllACLsOnResources(KafkaInstanceApi api, ServiceAccount serviceAccount, List<AclResourceType> resources) throws ApiGenericException {
        var principal = toPrincipal(serviceAccount.getClientId());

        // because ACLs that already exist are simply not created again, we do not need to check if the permission already exist.
        for (AclResourceType resourceType : resources) {
            createAllowAnyACL(api, principal, resourceType, AclOperation.ALL);
        }
    }

    public static boolean hasAllowAnyACL(List<AclBinding> acls, String principal, AclResourceType resourceType, AclOperation operation) {
        return acls.stream().anyMatch(a -> principal.equals(a.getPrincipal())
            && resourceType.equals(a.getResourceType())
            && AclPatternType.LITERAL.equals(a.getPatternType())
            && "*".equals(a.getResourceName())
            && AclPermissionType.ALLOW.equals(a.getPermission())
            && operation.equals(a.getOperation())
        );
    }

    public static boolean hasReadAnyTopicACL(List<AclBinding> acls, String principal) {
        return hasAllowAnyACL(acls, principal, AclResourceType.TOPIC, AclOperation.READ);
    }

    public static boolean hasWriteAnyTopicACL(List<AclBinding> acls, String principal) {
        return hasAllowAnyACL(acls, principal, AclResourceType.TOPIC, AclOperation.WRITE);
    }

    public static boolean hasReadAnyGroupACL(List<AclBinding> acls, String principal) {
        return hasAllowAnyACL(acls, principal, AclResourceType.GROUP, AclOperation.READ);
    }

    public static void applyProducerAndConsumerACLs(KafkaInstanceApi api, String principal) throws ApiGenericException {
        var aclPage = api.getAcls(null, null, null, principal, null, null, null, null, null, null);
        var acls = aclPage.getItems();

        if (!hasReadAnyGroupACL(acls, principal)) {
            createReadAnyGroupACL(api, principal);
        }

        if (!hasReadAnyTopicACL(acls, principal)) {
            createReadAnyTopicACL(api, principal);
        }

        if (!hasWriteAnyTopicACL(acls, principal)) {
            createWriteAnyTopicACL(api, principal);
        }
    }
}
