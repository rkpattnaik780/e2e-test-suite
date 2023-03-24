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

    private static final String BF2_DOMAIN = "bf2.org/";
    private static final String PROFILE_QUOTA_CONSUMED = BF2_DOMAIN + "kafkaInstanceProfileQuotaConsumed";
    private static final String PROFILE_TYPE = "bf2.org/kafkaInstanceProfileType";
    public static MixedOperation<ManagedKafka, KubernetesResourceList<ManagedKafka>, Resource<ManagedKafka>> managedKafka(KubernetesClient client) {
        return client.resources(ManagedKafka.class);
    }

    public static MixedOperation<ManagedKafkaAgent, KubernetesResourceList<ManagedKafkaAgent>, Resource<ManagedKafkaAgent>> managedKafkaAgent(KubernetesClient client) {
        return client.resources(ManagedKafkaAgent.class);
    }

    public static int getStreamingUnitCountOfExistingGivenManagedKafkaCRType(OpenShiftClient oc, ManagedKafkaType mkType) {
        return listManagedKafka(oc, mkType)
            .stream()
            .filter(e -> !e.getMetadata().getName().equals(getReservedDeploymentName(mkType)))
            .mapToInt(FleetshardUtils::getStreamingUnitOfMkInstance)
            .sum();
    }

    public static int getStreamingUnitOfMkInstance(ManagedKafka mkInstance) {
        return Integer.parseInt(mkInstance.getLabel(PROFILE_QUOTA_CONSUMED).orElse("1"));
    }

    public static List<ManagedKafka> listManagedKafka(OpenShiftClient oc, ManagedKafkaType mkType) {
        return FleetshardUtils.managedKafka(oc).inAnyNamespace().list().getItems()
            .stream()
            .filter(e -> mkType.toString().equals(e.getLabel(PROFILE_TYPE).orElse("not present")))
            .collect(Collectors.toList());
    }

    private static ManagedKafkaAgent getManagedKafkaAgent(OpenShiftClient oc) throws Throwable {
        ManagedKafkaAgent mkAgent = FleetshardUtils.managedKafkaAgent(oc).inAnyNamespace().list().getItems().stream()
            .findAny()
            .orElseThrow(() -> new Exception("managed kafka agent is not present in cluster"));
        log.trace(mkAgent);
        return mkAgent;
    }

    public static int getClusterCapacityFromMKAgent(OpenShiftClient oc, ManagedKafkaType mkType) throws Throwable {
        return FleetshardUtils.getManagedKafkaAgent(oc)
            .getStatus()
            .getCapacity()
            .get(mkType.toString())
            .getMaxUnits();
    }

    public static int getRemainingCapacityMKAgent(OpenShiftClient oc, ManagedKafkaType mkType) throws Throwable {
        return FleetshardUtils.getManagedKafkaAgent(oc)
            .getStatus()
            .getCapacity()
            .get(mkType.toString())
            .getRemainingUnits();
    }

    public  static Map<String, Integer> getReadyNodesPerEachMachineSetContainingName(OpenShiftClient oc, String containedName) {
        return oc.machine().machineSets().list().getItems().stream()
            .filter(e -> e.getMetadata().getName().contains(containedName))
            .collect(Collectors.toMap(m -> m.getMetadata().getName(), m -> m.getStatus().getReadyReplicas()));
    }

    public static String getReservedDeploymentName(ManagedKafkaType managedKafkaType) {
        return String.format("reserved-kafka-%s-1", managedKafkaType);
    }

}
