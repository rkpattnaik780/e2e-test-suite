package io.managed.services.test.cluster;

import com.openshift.cloud.api.kas.models.KafkaRequest;
import com.openshift.cloud.api.kas.models.KafkaRequestPayload;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.fabric8.openshift.client.OpenShiftClient;
import io.managed.services.test.Environment;
import io.managed.services.test.TestBase;
import io.managed.services.test.TestUtils;
import io.managed.services.test.client.ApplicationServicesApi;
import io.managed.services.test.client.exception.ApiGenericException;
import io.managed.services.test.client.kafkamgmt.KafkaClusterCapacityExhaustedException;
import io.managed.services.test.client.kafkamgmt.KafkaMgmtApi;
import io.managed.services.test.client.kafkamgmt.KafkaMgmtApiUtils;
import io.managed.services.test.dataplane.ManagedKafkaType;
import io.managed.services.test.k8.managedkafka.v1alpha1.ManagedKafka;
import io.managed.services.test.dataplane.FleetshardUtils;
import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j2;
import org.json.JSONObject;
import org.testng.Assert;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.ConfigBuilder;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;


// TODO unify and add env variables for gcp data plane clusters.
/**
 * <p>
 * Test state of data plane cluster by creating kafka instances and observing according change of state in
 * relevant custom resources.
 * <p>
 * <b>Tests:</b>
 * <ul>
 *     <li> testReservedDeploymentExistence: reserved deployment exists (developer and standard instances)
 *     <li> testReservedDeploymentDoesNotPreventKafkaCreation: reserved deployment is used to create actual kafka instance (developer and standard instances)
 *     <li> testStandardKafkaNodeAutoscaling: machineset resource is scaled when new instances are created (standard instance as developer are already at min max nodes value)
 *     <li> testCapacityReportingUpdated: managedkafkaagent reflect creation of new kafka instance in its remaining capacity (developer and standard instance)
 * </ul>
 * <p>
 * <b>Requires:</b>
 * <ul>
 *     <li> PRIMARY_OFFLINE_TOKEN
 * </ul>
 */
@Log4j2
public class DataPlaneClusterTest extends TestBase {


    static final String KAFKA_INSTANCE_NAME = "cl-e2e-" + Environment.LAUNCH_SUFFIX;

    private OpenShiftClient oc;

    private static int maxStandardStreamingUnitsInCluster;
    private static int maxDeveloperStreamingUnitsInCluster;
    private static final String KAFKAS_MGMT_21_CODE = "KAFKAS-MGMT-21";
    private static final String PLAN_STANDARD = "standard.x1";
    private static final String INSTANCE_NAME_PREFIX = "cl-e2e-placeholder-";
    private KafkaMgmtApi kafkaMgmtApi;

    @BeforeClass
    @SneakyThrows
    public void bootstrap() {

        log.info("build config");
        Config config = new ConfigBuilder()
            .withMasterUrl("https://api.mk-stage-0622.bd59.p1.openshiftapps.com:6443")
            .withOauthToken(Environment.PROMETHEUS_WEB_CLIENT_ACCESS_TOKEN)
            .withTrustCerts(true)
            .build();
        log.info("init openshift client");
        oc = new KubernetesClientBuilder().withConfig(config).build().adapt(OpenShiftClient.class);

        var apps = ApplicationServicesApi.applicationServicesApi(Environment.PRIMARY_OFFLINE_TOKEN);
        oc.pods().list();
        log.info(FleetshardUtils.managedKafkaAgent(oc).list().getItems());
        log.info(FleetshardUtils.managedKafka(oc).list().getItems());
        log.info(FleetshardUtils.managedKafkaAgent(oc).inAnyNamespace().list().getItems());

        log.info("init cluster capacity info");
        maxStandardStreamingUnitsInCluster = FleetshardUtils.getClusterCapacityFromMKAgent(oc, ManagedKafkaType.standard);
        log.debug("Max standard streaming units according to MKAgent capacity '{}'", maxStandardStreamingUnitsInCluster);
        maxDeveloperStreamingUnitsInCluster = FleetshardUtils.getClusterCapacityFromMKAgent(oc, ManagedKafkaType.developer);
        log.debug("Max developer streaming units according to MKAgent capacity '{}'", maxDeveloperStreamingUnitsInCluster);

        log.info("init kafka management");
        kafkaMgmtApi = apps.kafkaMgmt();
    }

    @AfterClass(alwaysRun = true)
    public void teardown() {

        // delete kafka instance
        try {
            KafkaMgmtApiUtils.cleanKafkaInstance(kafkaMgmtApi, KAFKA_INSTANCE_NAME);
        } catch (Throwable t) {
            log.error("clean main kafka instance error: ", t);
        }
    }

    @DataProvider(name = "managedKafkaTypes")
    public Object[][] managedKafkaTypeDataProvider() {
        return new ManagedKafkaType[][]{
            {ManagedKafkaType.standard},
            {ManagedKafkaType.developer}};
    }

    @Test(dataProvider = "managedKafkaTypes")
    @SneakyThrows
    public void testReservedDeploymentExistence(ManagedKafkaType mkType) {

        String namespace = String.format("reserved-kafka-%s-1", mkType);
        log.info("evaluating namespace '{}'", namespace);

        String managedKafkaName = String.format("reserved-kafka-%s-1", mkType);
        log.info("managed kafka reserved deployment '{}'", managedKafkaName);
        Optional<ManagedKafka> mkOptional = FleetshardUtils.managedKafka(oc).inNamespace(namespace).list().getItems().stream().filter(ManagedKafka::isReserveDeployment).findAny();
        Assert.assertTrue(mkOptional.isPresent(), "reserved deployment is not present");

        log.info("assert existence of resources which are part of reserved deployment");
        int expectedZookeeperPodsCount;
        int expectedKafkaPodsCount;
        switch (mkType.name()) {
            case "standard":
                expectedZookeeperPodsCount = 3;
                expectedKafkaPodsCount = 3;
                break;
            case "developer":
                expectedZookeeperPodsCount = 1;
                expectedKafkaPodsCount = 1;
                break;
            default:
                throw new Exception("unsupported managed kafka type");
        }

        log.info("test that '{}' Zookeeper Pod/s are present in namespace '{}'", expectedZookeeperPodsCount, namespace);
        int actualNumberOfZookeeperPods = countNumberOfPodsWithNameExisting(namespace, "zookeeper");
        Assert.assertEquals(expectedZookeeperPodsCount, actualNumberOfZookeeperPods, "unexpected number of zookeeper pods");

        log.info("test that '{}' Kafka Pod/s are present in namespace '{}'", expectedKafkaPodsCount, namespace);
        int actualNumberOfKafkaPods = countNumberOfPodsWithNameExisting(namespace, "zookeeper");
        Assert.assertEquals(expectedKafkaPodsCount, actualNumberOfKafkaPods, "unexpected number of zookeeper pods");

    }

    private int countNumberOfPodsWithNameExisting(String namespace, String nameSubstring) {
        log.debug("get pods count with nameSubstring '{}'  in namespace '{}'", nameSubstring, namespace);
        var podsPresent =  oc.pods().inNamespace(namespace).list().getItems().stream()
            .filter(e -> e.getMetadata().getName().contains(nameSubstring))
            .collect(Collectors.toList());
        log.debug(podsPresent);
        return podsPresent.size();
    }

    @Test(dataProvider = "managedKafkaTypes")
    public void testReservedDeploymentDoesNotPreventKafkaCreation(ManagedKafkaType mkType) throws Exception {

        log.info("testing managed kafka type: '{}'", mkType);

        // obtain max limit of kafka instances per given mk type (developer, standard)
        int upperStreamingUnitLimitPerManagedKafkaType;
        switch (mkType.name()) {
            case "standard":
                upperStreamingUnitLimitPerManagedKafkaType = maxStandardStreamingUnitsInCluster;
                break;
            case "developer":
                upperStreamingUnitLimitPerManagedKafkaType = maxDeveloperStreamingUnitsInCluster;
                break;
            default:
                throw new Exception("Unsupported managed kafka type");
        }
        log.info("upper streaming unit limit for instance type '{}' is '{}' instances.", mkType, upperStreamingUnitLimitPerManagedKafkaType);

        int currentStreamingUnitsCountOfGivenMkType = FleetshardUtils.getStreamingUnitCountOfExistingGivenManagedKafkaCRType(oc, mkType);
        log.info("currently there are '{}' existing streaming units of given type", currentStreamingUnitsCountOfGivenMkType);

        // if there are already max number of instances skip test
        if (currentStreamingUnitsCountOfGivenMkType > upperStreamingUnitLimitPerManagedKafkaType) {
            log.warn("currently too many streaming units ({}/{}), of type '{}'",
                currentStreamingUnitsCountOfGivenMkType,
                upperStreamingUnitLimitPerManagedKafkaType,
                mkType);
            throw new SkipException("Too many existing instances, which would cause quota to be breached");
        }

        // create kafka instance of expected type
        KafkaRequestPayload payload = new KafkaRequestPayload()
            .name(KAFKA_INSTANCE_NAME)
            .cloudProvider("aws")
            .region("us-east-1")
            .plan(String.format("%s.x1", mkType));

        log.info("attempt to create kafka instance");
        KafkaRequest kafkaRequest = null;
        try {
            kafkaRequest = kafkaMgmtApi.createKafka(true, payload);
            log.debug(kafkaRequest);
            log.info("wait for provisioning of kafka instance with id '{}'", kafkaRequest.getId());
            KafkaMgmtApiUtils.waitUntilKafkaIsReady(kafkaMgmtApi, kafkaRequest.getId());
        } catch (ApiGenericException e) {
            // some users may not be able to create some types of instances, e.g., user with quota will not be able to create dev. instance
            log.warn(e);
            JSONObject jsonResponse = new JSONObject(e.getResponseBody());
            if (KAFKAS_MGMT_21_CODE.equals(jsonResponse.get("code")))
                throw new SkipException(String.format("user %s has no quota to create instance of type %s", Environment.PRIMARY_USERNAME, mkType));
            else
                throw  e;
        } finally {
            // cleanup of kafka instance
            log.info("clean kafka instance with name '{}'", KAFKA_INSTANCE_NAME);
            try {
                KafkaMgmtApiUtils.deleteKafkaByNameIfExists(kafkaMgmtApi, KAFKA_INSTANCE_NAME);
                log.info("wait until kafka is deleted");
                KafkaMgmtApiUtils.waitUntilKafkaIsDeleted(kafkaMgmtApi, kafkaRequest.getId());
            } catch (Exception e) {
                log.error("error while cleaning kafka instance: %s", e);
            }
        }
    }

    // TODO node downscaling
    @Test()
    @SneakyThrows
    public void testStandardKafkaNodeAutoscaling() {

        //take a snapshot of machine sets with name (INITIAL)
        Map<String, Integer> machineSetToReadyNodeCountSnapshotBefore =  FleetshardUtils.getReadyNodesPerEachMachineSetContainingName(oc, "kafka-standard");
        log.info(machineSetToReadyNodeCountSnapshotBefore);

        // obtain info how many remaining standard streaming units there are according to capacity
        int remainingStandardInstancesCapacity = FleetshardUtils.getCapacityRemainingUnitsFromMKAgent(oc, ManagedKafkaType.standard);
        log.info("currently remaining standard managed kafka instances capacity '{}'", remainingStandardInstancesCapacity);

        // there is not enough space in the cluster to observe nodes scaling.
        if (remainingStandardInstancesCapacity == 0)
            throw new SkipException("Already reached max limit of standard instances created");

        // creating one instance less than to reach max as that would not help observing max node creation anyway.
        log.info("creating at least '{}' standard managed kafka instances", remainingStandardInstancesCapacity);

        // wait up to 15 minutes for observing increase in nodes number in relevant machine sets, or creation of new instance
        AtomicInteger placeholderInstanceSuffixCounter = new AtomicInteger(1);

        try {
            TestUtils.waitFor(
                "update of number of nodes scaled due to creation of new standard managed kafka instance",
                Duration.ofSeconds(10),
                Duration.ofMinutes(15),
                ready -> {
                    int remainingCapacity = FleetshardUtils.getCapacityRemainingUnitsFromMKAgent(oc, ManagedKafkaType.standard);
                    log.info("remaining capacity in the cluster '{}'", remainingCapacity);

                    // spawn new instance if there is a capacity for it
                    int minNodeInMachineSet =  machineSetToReadyNodeCountSnapshotBefore.values().stream().reduce(0, Integer::min);

                    // if we did not reach max capacity, and at least of machineSet was not already at highest number we attempt creating new instances (quite common occurrence as downscaling take much longer time)
                    if (remainingCapacity > 0 && minNodeInMachineSet < maxStandardStreamingUnitsInCluster) {
                        log.info("creating an instance");
                        var payload = new KafkaRequestPayload()
                            .name(INSTANCE_NAME_PREFIX + placeholderInstanceSuffixCounter.getAndIncrement() + "-" + Environment.LAUNCH_KEY)
                            .plan(PLAN_STANDARD)
                            .cloudProvider(Environment.CLOUD_PROVIDER)
                            .region(Environment.DEFAULT_KAFKA_REGION);
                        try {
                            KafkaMgmtApiUtils.attemptCreatingKafkaInstance(kafkaMgmtApi, payload, Duration.ofSeconds(20), Duration.ofSeconds(20));
                        } catch (KafkaClusterCapacityExhaustedException e) {
                            log.warn("capacity exhausted at the moment %s", e);
                        }
                    }

                    // observe if newly observed value in nodes increased from original snapshot
                    Map<String, Integer> machineSetToReadyNodeCountSnapshotCurrent = FleetshardUtils.getReadyNodesPerEachMachineSetContainingName(oc, "kafka-standard");
                    for (var currentlyObservedMachineSet  : machineSetToReadyNodeCountSnapshotCurrent.entrySet()) {
                        // naming
                        String machineSetName = currentlyObservedMachineSet.getKey();
                        int newlyObservedValue = currentlyObservedMachineSet.getValue();
                        int oldObservedValue = machineSetToReadyNodeCountSnapshotBefore.get(currentlyObservedMachineSet.getKey());

                        // observing if value changed
                        log.debug("machineSet '{}' capacity before '{}' and now '{}'", machineSetName, oldObservedValue, newlyObservedValue);
                        if (newlyObservedValue > oldObservedValue) {
                            log.info("MachineSet '{}' changed scaled number of its nodes", machineSetName);
                            return true;
                        }
                    }

                    // observe if any of newly crated kafka instance really is ready state (node for sure scaled), otherwise continue waiting
                    return kafkaMgmtApi
                        .getKafkas(null, null, null, null)
                        .getItems().stream()
                        .filter(e -> e.getName().contains(INSTANCE_NAME_PREFIX))
                        .filter(e -> e.getOwner().equals(Environment.PRIMARY_USERNAME))
                        .anyMatch(e -> e.getStatus().equals("ready"));
                }
            );

        } catch (Exception ignored) {
        } finally {
            // delete and wait for cleaning of all instances spawned
            KafkaMgmtApiUtils.deleteSearchedKafkaInstancesByOwner(kafkaMgmtApi, INSTANCE_NAME_PREFIX, Environment.PRIMARY_USERNAME);
        }
    }

    @Test(dataProvider = "managedKafkaTypes")
    @SneakyThrows
    public void testReportedCapacityMatchesNumberOfExistingInstances(ManagedKafkaType mkType) {

        int observedRemainingCapacityInitially = FleetshardUtils.getCapacityRemainingUnitsFromMKAgent(oc, mkType);
        log.info("observed remaining capacity before creating new instance '{}'", observedRemainingCapacityInitially);
        if (observedRemainingCapacityInitially == 0)
            throw new SkipException("cluster has no free capacity to test at the moment");

        Set<String> kafkaInstanceNamesBeforeCreating = FleetshardUtils.listManagedKafka(oc, mkType).stream()
            .map(e -> e.getMetadata().getName())
            .collect(Collectors.toSet());
        log.debug("existing mk instances before creating a new one {}", kafkaInstanceNamesBeforeCreating);

        // create kafka instance of expected type
        log.info(KAFKA_INSTANCE_NAME + "-" + mkType);
        KafkaRequestPayload payload = new KafkaRequestPayload()
                .name(KAFKA_INSTANCE_NAME + "-" + mkType)
                .cloudProvider("aws")
                .region("us-east-1")
                .plan(String.format("%s.x1", mkType));

        log.info("attempt to create kafka instance");
        KafkaRequest kafkaRequest = null;
        try {
            kafkaRequest = kafkaMgmtApi.createKafka(true, payload);
            log.debug(kafkaRequest);
            log.info("wait for provisioning of kafka instance with id '{}'", kafkaRequest.getId());
            KafkaMgmtApiUtils.waitUntilKafkaIsProvisioning(kafkaMgmtApi, kafkaRequest.getId());

            // wait for three minutes to see if reported capacity already increased (exclusively)
            TestUtils.waitFor(
                "update reported remaining capacity",
                Duration.ofSeconds(5),
                Duration.ofMinutes(2),
                ready -> {
                    int newlyObservedRemainingCapacity = FleetshardUtils.getCapacityRemainingUnitsFromMKAgent(oc, mkType);
                    log.debug("newly observed remaining capacity '{}'", newlyObservedRemainingCapacity);
                    if (newlyObservedRemainingCapacity < observedRemainingCapacityInitially)
                        return true;
                    return false;
                });

            // get number of instances that were deleted while waiting for provisioning of new kafka instance
            List<String> kafkaInstanceNamesAfterCreating = FleetshardUtils.listManagedKafka(oc, mkType).stream()
                .map(e -> e.getMetadata().getName())
                .collect(Collectors.toList());
            List<String> deletedInstances = kafkaInstanceNamesBeforeCreating.stream().filter(e -> !kafkaInstanceNamesAfterCreating.contains(e)).collect(Collectors.toList());
            log.debug("count of instances that have been deleted since begging of the test {}", deletedInstances.size());

            int newlyObservedRemainingCapacity = FleetshardUtils.getCapacityRemainingUnitsFromMKAgent(oc, mkType);
            // remaining capacity should be lowered by 1, but as there are instances which may have been deleted by other users (which would free some space) we are not including them
            Assert.assertTrue(newlyObservedRemainingCapacity <= observedRemainingCapacityInitially - 1  +  deletedInstances.size());
        } catch (ApiGenericException e) {
            // some users may not be able to create some types of instances, e.g., user with quota will not be able to create dev. instance
            log.warn(e);
            JSONObject jsonResponse = new JSONObject(e.getResponseBody());
            if (KAFKAS_MGMT_21_CODE.equals(jsonResponse.get("code")))
                throw new SkipException(String.format("user %s has no quota to create instance of type %s", Environment.PRIMARY_USERNAME, mkType));
            else
                throw  e;
        } finally {
            // cleanup of kafka instance
            log.info("clean kafka instance with name '{}'", KAFKA_INSTANCE_NAME + "-" + mkType);
            try {
                // list all instances which exist before we delete our instance
                List<String> kafkaInstanceNamesBeforeDeleting = FleetshardUtils.listManagedKafka(oc, mkType).stream()
                    .map(e -> e.getMetadata().getName())
                    .collect(Collectors.toList());

                // get remaining capacity before
                int remainingCapacityBefore = FleetshardUtils.getCapacityRemainingUnitsFromMKAgent(oc, mkType);

                // delete our instance
                KafkaMgmtApiUtils.deleteKafkaByNameIfExists(kafkaMgmtApi, KAFKA_INSTANCE_NAME + "-" + mkType);
                log.info("wait until kafka is deleted");
                KafkaMgmtApiUtils.waitUntilKafkaIsDeleted(kafkaMgmtApi, kafkaRequest.getId());

                // waiting for ideal case (capacity updated accordingly, and no other user created new kafka instance)
                TestUtils.waitFor(
                    "update (increase) reported remaining capacity",
                    Duration.ofSeconds(5),
                    Duration.ofMinutes(2),
                    ready -> {
                        int newlyObservedRemainingCapacity = FleetshardUtils.getCapacityRemainingUnitsFromMKAgent(oc, mkType);
                        log.debug("newly observed remaining capacity '{}'", newlyObservedRemainingCapacity);
                        if (newlyObservedRemainingCapacity > remainingCapacityBefore)
                            return true;
                        return false;
                    });

                // list all instances that exist after our instance was deleted
                List<String> kafkaInstanceNamesAfterDeleting = FleetshardUtils.listManagedKafka(oc, mkType).stream()
                    .map(e -> e.getMetadata().getName())
                    .collect(Collectors.toList());

                // observe how many new instances of given type were created while we were deleting our instance (increasing free capacity by one)
                int countNewlyCreatedInstances = kafkaInstanceNamesAfterDeleting.stream().filter(e -> !kafkaInstanceNamesBeforeDeleting.contains(e)).collect(Collectors.toList()).size();
                log.debug("number of newly created kafka instances while waiting to increase free capacity '{}'", countNewlyCreatedInstances);

                // get free capacity after (kafka deletion)
                int remainingCapacityAfter = FleetshardUtils.getCapacityRemainingUnitsFromMKAgent(oc, mkType);
                log.debug("reported remaining free capacity after kafka deletion '{}'", remainingCapacityAfter);

                // if no new kafka were created we expect remaining (free) capacity to be higher than before deletion of 1 instance
                // condition is eased with each new kafka instance that was created while we were waiting for capacity to reflect deletion of 1 instance.
                Assert.assertTrue(remainingCapacityAfter + countNewlyCreatedInstances > remainingCapacityBefore);

            } catch (Exception e) {
                log.error("error while cleaning kafka instance: %s", e);
            }
        }
    }

}
