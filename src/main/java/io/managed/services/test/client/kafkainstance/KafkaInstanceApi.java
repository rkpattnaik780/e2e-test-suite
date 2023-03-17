package io.managed.services.test.client.kafkainstance;

import com.openshift.cloud.api.kas.auth.ApiClient;
import com.openshift.cloud.api.kas.auth.api.v1.V1RequestBuilder;
import com.openshift.cloud.api.kas.auth.models.AclBinding;
import com.openshift.cloud.api.kas.auth.models.AclBindingListPage;
import com.openshift.cloud.api.kas.auth.models.AclOperation;
import com.openshift.cloud.api.kas.auth.models.AclPatternType;
import com.openshift.cloud.api.kas.auth.models.AclPermissionType;
import com.openshift.cloud.api.kas.auth.models.AclResourceType;
import com.openshift.cloud.api.kas.auth.models.ConsumerGroup;
import com.openshift.cloud.api.kas.auth.models.ConsumerGroupList;
import com.openshift.cloud.api.kas.auth.models.NewTopicInput;
import com.openshift.cloud.api.kas.auth.models.Topic;
import com.openshift.cloud.api.kas.auth.models.TopicSettings;
import com.openshift.cloud.api.kas.auth.models.TopicsList;
import io.managed.services.test.client.BaseApi;
import io.managed.services.test.client.exception.ApiGenericException;
import io.managed.services.test.client.exception.ApiUnknownException;
import lombok.extern.log4j.Log4j2;
import java.util.concurrent.TimeUnit;

@Log4j2
public class KafkaInstanceApi extends BaseApi {

    private final ApiClient apiClient;
    private final V1RequestBuilder v1;

    public KafkaInstanceApi(ApiClient apiClient, String offlineToken) {
        super(offlineToken);
        this.apiClient = apiClient;
        this.v1 = apiClient.api().v1();
    }

    @Override
    protected ApiUnknownException toApiException(Exception e) {
        log.info(e);

        if (e.getCause() != null) {
            if (e.getCause() instanceof com.openshift.cloud.api.kas.auth.models.Error) {
                var err = (com.openshift.cloud.api.kas.auth.models.Error) e.getCause();
                return new ApiUnknownException(err.getReason(), err.getCode().toString(), err.responseStatusCode, err.getHref(), err.getId(), err);
            }
            if (e.getCause() instanceof com.openshift.cloud.api.kas.models.Error) {
                var err = (com.openshift.cloud.api.kas.models.Error) e.getCause();
                return new ApiUnknownException(err.getReason(), err.getCode(), err.responseStatusCode, err.getHref(), err.getId(), err);
            }
        }

        return null;

    }

    public Topic updateTopic(String name, TopicSettings ts) throws ApiGenericException {
        //return getTopics(null, null, null, null, null);
        return retry(() -> v1.topics(name).patch(ts).get(10, TimeUnit.SECONDS));
    }

    public TopicsList getTopics() throws ApiGenericException {
        return getTopics(null, null, null, null, null);
    }

    public TopicsList getTopics(Integer size, Integer page, String filter, String order, String orderKey) throws ApiGenericException {
        return retry(() -> v1.topics().get(config -> {
            config.queryParameters.size = size;
            config.queryParameters.page = page;
            config.queryParameters.filter = filter;
            config.queryParameters.order = order;
            config.queryParameters.orderKey = orderKey;
        }).get(10, TimeUnit.SECONDS));
    }

    public Topic getTopic(String topicName) throws ApiGenericException {
        return retry(() -> v1.topics(topicName).get().get(10, TimeUnit.SECONDS));
    }

    public Topic createTopic(NewTopicInput newTopicInput) throws ApiGenericException {
        return retry(() -> v1.topics().post(newTopicInput).get(10, TimeUnit.SECONDS));
    }

    public void deleteTopic(String topicName) throws ApiGenericException {
        retry(() -> v1.topics(topicName).delete().get(10, TimeUnit.SECONDS));
    }

    public ConsumerGroupList getConsumerGroups() throws ApiGenericException {
        return getConsumerGroups(null, null, null, null, null, null);
    }

    public ConsumerGroupList getConsumerGroups(Integer size, Integer page, String topic, String groupIdFilter, String order, String orderKey) throws ApiGenericException {
        return retry(() -> v1.consumerGroups().get(config -> {
            config.queryParameters.size = size;
            config.queryParameters.page = page;
            config.queryParameters.groupIdFilter = groupIdFilter;
            config.queryParameters.order = order;
            config.queryParameters.orderKey = orderKey;
        }).get(10, TimeUnit.SECONDS));
    }

    public ConsumerGroup getConsumerGroupById(String consumerGroupId) throws ApiGenericException {
        return getConsumerGroupById(consumerGroupId, null, null, null, null);
    }

    public ConsumerGroup getConsumerGroupById(String consumerGroupId, String order, String orderKey, Integer partitionFilter, String topic) throws ApiGenericException {
        return retry(() -> v1.consumerGroups(consumerGroupId).get(config -> {
            config.queryParameters.order = order;
            config.queryParameters.orderKey = orderKey;
            config.queryParameters.partitionFilter = partitionFilter;
            config.queryParameters.topic = topic;
        }).get(10, TimeUnit.SECONDS));
    }

    public void deleteConsumerGroupById(String consumerGroupId) throws ApiGenericException {
        retry(() -> v1.consumerGroups(consumerGroupId).delete().get(10, TimeUnit.SECONDS));
    }

    public AclBindingListPage getAcls(String resourceType, String resourceName, String patternType, String principal, String operation, String permission, Integer page, Integer size, String order, String orderKey) throws ApiGenericException {
        return retry(() -> v1.acls().get(config -> {
            config.queryParameters.resourceType = resourceType;
            config.queryParameters.resourceName = resourceName;
            config.queryParameters.patternType = patternType;
            config.queryParameters.principal = principal;
            config.queryParameters.operation = operation;
            config.queryParameters.permission = permission;
            config.queryParameters.page = page;
            config.queryParameters.size = size;
            config.queryParameters.order = order;
            config.queryParameters.orderKey = orderKey;
        }).get(10, TimeUnit.SECONDS));
    }

    public void createAcl(AclBinding aclBinding) throws ApiGenericException {
        retry(() -> v1.acls().post(aclBinding).get(10, TimeUnit.SECONDS));
    }

    public AclBindingListPage deleteAcls(AclResourceType resourceType, String resourceName, AclPatternType patternType, String principal, AclOperation operation, AclPermissionType permission) throws ApiGenericException {
        return retry(() -> v1.acls().delete(config -> {
            config.queryParameters.resourceType = resourceType.name();
            config.queryParameters.resourceName = resourceName;
            config.queryParameters.patternType = patternType.name();
            config.queryParameters.principal = principal;
            config.queryParameters.operation = operation.name();
            config.queryParameters.permission = permission.name();
        }).get(10, TimeUnit.SECONDS));
    }
}
