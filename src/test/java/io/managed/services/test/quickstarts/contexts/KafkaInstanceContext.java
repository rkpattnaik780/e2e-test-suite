package io.managed.services.test.quickstarts.contexts;

import com.openshift.cloud.api.kas.models.KafkaRequest;
import io.managed.services.test.Environment;
import io.managed.services.test.client.kafkainstance.KafkaInstanceApi;
import io.managed.services.test.client.kafkainstance.KafkaInstanceApiUtils;
import lombok.Getter;
import lombok.Setter;

import java.util.Objects;

@Getter
@Setter
public class KafkaInstanceContext {

    private final UserContext userContext;

    private KafkaRequest kafkaInstance;

    public KafkaInstanceContext(UserContext userContext) {
        this.userContext = userContext;
    }

    public KafkaRequest requireKafkaInstance() {
        return Objects.requireNonNull(kafkaInstance);
    }

    /**
     * This method requires the Kafka Instance and the MAS SSO User to be initialized.
     *
     * @return the KafkaInstanceApi for the Kafka instance in context
     */
    public KafkaInstanceApi kafkaInstanceApi() {
        var masUser = userContext.requireMasUser();
        var kafkaInstance = this.requireKafkaInstance();
        // TODO: check if this is the correct token to be used
        return KafkaInstanceApiUtils.kafkaInstanceApi(kafkaInstance, Environment.PRIMARY_OFFLINE_TOKEN);
    }
}
