package io.managed.services.test.dataplane;

import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.openshift.client.OpenShiftClient;
import io.managed.services.test.k8.managedkafka.v1alpha1.ManagedKafka;
import io.managed.services.test.k8.managedkafka.v1alpha1.ManagedKafkaAgent;
import lombok.extern.log4j.Log4j2;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Log4j2
public class FleetshardUtils {

    public static MixedOperation<ManagedKafka, KubernetesResourceList<ManagedKafka>, Resource<ManagedKafka>> managedKafka(KubernetesClient client) {
        return client.resources(ManagedKafka.class);
    }

    public static MixedOperation<ManagedKafkaAgent, KubernetesResourceList<ManagedKafkaAgent>, Resource<ManagedKafkaAgent>> managedKafkaAgent(KubernetesClient client) {
        return client.resources(ManagedKafkaAgent.class);
    }

    public static int getCountOfExistingGivenManagedKafkaCRType(OpenShiftClient oc, ManagedKafkaType mkType) {
        return (int) FleetshardUtils.managedKafka(oc).inAnyNamespace().list().getItems()
            .stream()
            .filter(e -> mkType.toString().equals(e.getLabel("bf2.org/kafkaInstanceProfileType").orElse("notPresent")))
            .count();
    }

    public static List<ManagedKafka> listManagedKafka(OpenShiftClient oc, ManagedKafkaType mkType) {
        return FleetshardUtils.managedKafka(oc).inAnyNamespace().list().getItems()
                .stream()
                .filter(e -> mkType.toString().equals(e.getLabel("bf2.org/kafkaInstanceProfileType").orElse("not present")))
                .collect(Collectors.toList());
    }

    private static ManagedKafkaAgent getManagedKafka(OpenShiftClient oc) throws Throwable {
        ManagedKafkaAgent mkAgent = FleetshardUtils.managedKafkaAgent(oc).list().getItems().stream()
            .findAny()
            .orElseThrow(() -> new Exception("managed kafka agent is not present in cluster"));
        log.trace(mkAgent);
        return mkAgent;
    }

    public static int getClusterCapacityFromMKAgent(OpenShiftClient oc, ManagedKafkaType mkType) throws Throwable {
        return FleetshardUtils.getManagedKafka(oc)
                .getStatus()
                .getCapacity()
                .get(mkType.toString())
                .getMaxUnits();
    }

    public static int getCapacityRemainingUnitsFromMKAgent(OpenShiftClient oc, ManagedKafkaType mkType) throws Throwable {
        return FleetshardUtils.getManagedKafka(oc)
                .getStatus()
                .getCapacity()
                .get(mkType.toString())
                .getRemainingUnits();
    }

    public  static Map<String, Integer> getReadyNodesPerEachMachineSetContainingName(OpenShiftClient oc, String containedName) {
        return oc.machine().machineSets().list().getItems().stream()
                .filter(e -> e.getMetadata().getName().contains("kafka-standard"))
                .collect(Collectors.toMap(m -> m.getMetadata().getName(), m -> m.getStatus().getReadyReplicas()));
    }

}
