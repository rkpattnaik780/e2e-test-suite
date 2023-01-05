package io.managed.services.test.k8.operator.resources.v1alpha1;

import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ManagedKafkaUtils {
    private static final Logger LOGGER = LogManager.getLogger(ManagedKafkaUtils.class);

    public static MixedOperation<ManagedKafka, KubernetesResourceList<ManagedKafka>, Resource<ManagedKafka>> managedKafka(KubernetesClient client) {
        return client.resources(ManagedKafka.class);
    }

}
