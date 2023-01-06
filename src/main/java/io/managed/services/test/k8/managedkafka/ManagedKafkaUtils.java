package io.managed.services.test.k8.managedkafka;

import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.openshift.client.OpenShiftClient;
import io.managed.services.test.k8.managedkafka.resources.v1alpha1.ManagedKafka;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ManagedKafkaUtils {
    private static final Logger LOGGER = LogManager.getLogger(ManagedKafkaUtils.class);

    public static MixedOperation<ManagedKafka, KubernetesResourceList<ManagedKafka>, Resource<ManagedKafka>> managedKafka(KubernetesClient client) {
        return client.resources(ManagedKafka.class);
    }

    public static int getCountOfExistingGivenManagedKafkaType(OpenShiftClient oc, String mkType) {

        return (int) ManagedKafkaUtils.managedKafka(oc).inAnyNamespace().list().getItems()
            .stream()
            .filter(e -> mkType.equals(e.getLabel("bf2.org/kafkaInstanceProfileType").orElse("notPresent")))
            .count();
    }
}
