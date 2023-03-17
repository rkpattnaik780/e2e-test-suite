package io.managed.services.test.client.kafkamgmt;

import com.openshift.cloud.api.kas.ApiClient;
import com.openshift.cloud.api.kas.api.kafkas_mgmt.v1.V1RequestBuilder;
import com.openshift.cloud.api.kas.api.kafkas_mgmt.v1.kafkas.item.metrics.query.MetricsInstantQueryListResponse;
import com.openshift.cloud.api.kas.models.KafkaRequest;
import com.openshift.cloud.api.kas.models.KafkaRequestList;
import com.openshift.cloud.api.kas.models.KafkaRequestPayload;
import com.openshift.cloud.api.kas.models.KafkaUpdateRequest;
import io.managed.services.test.client.BaseApi;
import io.managed.services.test.client.exception.ApiGenericException;
import io.managed.services.test.client.exception.ApiUnknownException;
import lombok.extern.log4j.Log4j2;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

@Log4j2
public class KafkaMgmtApi extends BaseApi {

    private final ApiClient apiClient;
    private final V1RequestBuilder v1;

    public KafkaMgmtApi(ApiClient apiClient, String offlineToken) {
        super(offlineToken);
        this.apiClient = Objects.requireNonNull(apiClient);
        this.v1 = apiClient.api().kafkas_mgmt().v1();
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

    public KafkaRequest getKafkaById(String id) throws ApiGenericException {
        return retry(() -> v1.kafkas(id).get().get(10, TimeUnit.SECONDS));
    }

    public KafkaRequestList getKafkas(String page, String size, String orderBy, String search) throws ApiGenericException {
        return retry(() -> v1.kafkas().get(config -> {
            config.queryParameters.page = page;
            config.queryParameters.size = size;
            config.queryParameters.orderBy = orderBy;
            config.queryParameters.search = search;
        }).get(10, TimeUnit.SECONDS));
    }

    public KafkaRequest createKafka(Boolean async, KafkaRequestPayload kafkaRequestPayload) throws ApiGenericException {
        return retry(() -> v1.kafkas()
                        .post(kafkaRequestPayload, config -> config.queryParameters.async = async).get(10, TimeUnit.SECONDS));
    }

    public void deleteKafkaById(String id, Boolean async) throws ApiGenericException {
        // TODO: why does it return Error
        retry(() -> v1.kafkas(id).delete(config -> config.queryParameters.async = async).get(10, TimeUnit.SECONDS));
    }

    public MetricsInstantQueryListResponse getMetricsByInstantQuery(String id, List<String> filters) throws ApiGenericException {
        return retry(() -> v1.kafkas(id).metrics().query().get(config -> {
            config.queryParameters.filters = filters.toArray(new String[0]);
        }).get(10, TimeUnit.SECONDS));
    }

    public String federateMetrics(String id) throws ApiGenericException {
        return retry(() -> v1.kafkas(id).metrics().federate().get().get(10, TimeUnit.SECONDS));
    }

    public KafkaRequest updateKafka(String instanceId, KafkaUpdateRequest kafkaUpdateRequest) throws ApiGenericException {
        return retry(() -> v1.kafkas(instanceId).patch(kafkaUpdateRequest).get(10, TimeUnit.SECONDS));
    }
}
