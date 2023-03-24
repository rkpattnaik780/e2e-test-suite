package io.managed.services.test.cluster;

import com.openshift.cloud.api.kas.models.KafkaRequest;
import com.openshift.cloud.api.kas.models.KafkaRequestPayload;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.fabric8.openshift.client.OpenShiftClient;
import io.managed.services.test.Environment;
import io.managed.services.test.TestBase;
import io.managed.services.test.TestUtils;
import io.managed.services.test.client.ApplicationServicesApi;
import io.managed.services.test.client.exception.ApiForbiddenException;
import io.managed.services.test.client.exception.ApiGenericException;
import io.managed.services.test.client.kafkamgmt.KafkaClusterCapacityExhaustedException;
import io.managed.services.test.client.kafkamgmt.KafkaMgmtApi;
import io.managed.services.test.client.kafkamgmt.KafkaMgmtApiUtils;
import io.managed.services.test.dataplane.ManagedKafkaType;
import io.managed.services.test.k8.managedkafka.v1alpha1.ManagedKafka;
import io.managed.services.test.dataplane.FleetshardUtils;
import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j2;
import org.testng.Assert;
import org.testng.SkipException;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.AfterClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.ConfigBuilder;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import org.json.JSONObject;

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

    private static final String KAFKAS_MGMT_120_CODE = "KAFKAS-MGMT-120";
    private static final String KAFKAS_MGMT_21_CODE = "KAFKAS-MGMT-21";
    private static final String KAFKAS_MGMT_24_CODE = "KAFKAS-MGMT-24";
    private static final String PLAN_STANDARD = "standard.x1";
    private static final String DUMMY_KAFKA_INSTANCE_NAME = "cl-e2e-placeholder-" + Environment.LAUNCH_KEY;
    private KafkaMgmtApi kafkaMgmtApi;

    @BeforeClass(alwaysRun = true)
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

        // validate oc
        oc.pods().list();

        var apps = ApplicationServicesApi.applicationServicesApi(Environment.PRIMARY_OFFLINE_TOKEN);

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
        switch (mkType) {
            case standard:
                expectedZookeeperPodsCount = 3;
                expectedKafkaPodsCount = 3;
                break;
            case developer:
                expectedZookeeperPodsCount = 1;
                expectedKafkaPodsCount = 1;
                break;
            default:
                throw new Exception("unsupported managed kafka type");
        }

        log.info("test that '{}' Zookeeper Pod/s are present in namespace '{}'", expectedZookeeperPodsCount, namespace);
        int actualNumberOfZookeeperPods = countNumberOfPodsWithNameExisting(namespace, namespace + "-zookeeper");
        Assert.assertEquals(actualNumberOfZookeeperPods, expectedZookeeperPodsCount, "unexpected number of zookeeper pods");

        log.info("test that '{}' Kafka Pod/s are present in namespace '{}'", expectedKafkaPodsCount, namespace);
        int actualNumberOfKafkaPods = countNumberOfPodsWithNameExisting(namespace, namespace + "-kafka");
        Assert.assertEquals(actualNumberOfKafkaPods, expectedKafkaPodsCount, "unexpected number of kafka pods");

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
    public void testReservedDeploymentDoesNotPreventKafkaCreation(ManagedKafkaType mkType) throws Throwable {

        log.info("testing managed kafka type: '{}'", mkType);

        // obtain max limit of kafka instances per given mk type (developer, standard)
        int upperStreamingUnitLimitPerManagedKafkaType = FleetshardUtils.getClusterCapacityFromMKAgent(oc, mkType);
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
        KafkaRequestPayload payload = new KafkaRequestPayload();
        payload.setName(KAFKA_INSTANCE_NAME);
        payload.setCloudProvider("aws");
        payload.setRegion("us-east-1");
        payload.setPlan(String.format("%s.x1", mkType));

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
            if (KAFKAS_MGMT_21_CODE.equals(new JSONObject(e.getResponseBody()).get("code")))
                throw new SkipException(String.format("user %s has no quota to create instance of type %s", Environment.PRIMARY_USERNAME, mkType));
            if (KAFKAS_MGMT_24_CODE.equals(new JSONObject(e.getResponseBody()).get("code")))
                throw new SkipException(String.format("user %s cannot create instance of type %s, due to cluster max capacity", Environment.PRIMARY_USERNAME, mkType));
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

    // TODO node downscaling, see https://github.com/bf2fc6cc711aee1a0c2a/e2e-test-suite/pull/448#discussion_r1090303391 for the related discussion
    @Test()
    @SneakyThrows
    public void testStandardKafkaNodeAutoscaling() {

        //take a snapshot of machine sets with name (INITIAL)
        Map<String, Integer> machineSetToReadyNodeCountSnapshotBefore =  FleetshardUtils.getReadyNodesPerEachMachineSetContainingName(oc, "kafka-standard");
        log.debug(machineSetToReadyNodeCountSnapshotBefore);
        // find out the minimum nodes value per machine set
        int minNodeInMachineSet =  machineSetToReadyNodeCountSnapshotBefore.values().stream().mapToInt(Integer::valueOf).min().orElse(0);
        log.info("currently minimum existing nodes in machine set '{}'", minNodeInMachineSet);

        // get remaining capacity from MK agent
        int remainingCapacity = FleetshardUtils.getRemainingCapacityMKAgent(oc, ManagedKafkaType.standard);
        log.info("remaining capacity in the cluster '{}'", remainingCapacity);

        // if no free capacity remains skip test
        if (remainingCapacity == 0)
            throw new SkipException("cluster already reported to be at its maximum capacity");

        log.info("creating an instance");
        var payload = new KafkaRequestPayload();
        payload.setName(DUMMY_KAFKA_INSTANCE_NAME);
        payload.setPlan(PLAN_STANDARD);
        payload.setCloudProvider(Environment.CLOUD_PROVIDER);
        payload.setRegion(Environment.DEFAULT_KAFKA_REGION);

        try {
            KafkaRequest kafkaRequest = KafkaMgmtApiUtils.attemptCreatingKafkaInstance(kafkaMgmtApi, payload, Duration.ofSeconds(20), Duration.ofSeconds(20));
            log.debug(kafkaRequest);

            // wait either for new node to be scaled, or kafka instance to be in ready state if there are instances being deleted in the cluster
            TestUtils.waitFor(
                "update of number of nodes scaled due to creation of new standard managed kafka instance",
                Duration.ofSeconds(10),
                Duration.ofMinutes(15),
                ready -> {
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

                    // if new some of original instances was deleted, we only wait for instance to be at least in ready state
                    // observe if any of newly crated kafka instance really is ready state (node for sure scaled), otherwise continue waiting
                    KafkaRequest currentKafka = KafkaMgmtApiUtils.getKafkaByName(kafkaMgmtApi, DUMMY_KAFKA_INSTANCE_NAME).get();
                    log.debug(currentKafka);
                    return currentKafka.getStatus().equals("ready");
                }
            );

        } catch (ApiForbiddenException e) {
            // if not quota related exception rethrow it
            if (!(e.getCode() == 403 && new JSONObject(e.getResponseBody()).get("code").equals(KAFKAS_MGMT_120_CODE))) {
                throw e;
            }
            log.warn("quota reached %s", e);
            throw new SkipException("standard kafka instance quota reached");

        } catch (KafkaClusterCapacityExhaustedException e) {
            log.warn("capacity exhausted at the moment %s", e);
            throw new SkipException("cluster capacity for standard kafka instances in aws data plane cluster reached");
        } catch (TimeoutException e) {
            log.warn("kafka not in a ready state");
            log.warn(KafkaMgmtApiUtils.getKafkaByName(kafkaMgmtApi, DUMMY_KAFKA_INSTANCE_NAME).get());
            throw e;
        } finally {
            // delete and wait for cleaning of all instances spawned
            KafkaMgmtApiUtils.deleteKafkaByNameIfExists(kafkaMgmtApi, DUMMY_KAFKA_INSTANCE_NAME);
        }
    }

    @Test(dataProvider = "managedKafkaTypes")
    @SneakyThrows
    public void testReportedMKCapacity(ManagedKafkaType mkType) {

        log.info("test reported capacity for {} instances", mkType);

        // obtain reported consumed capacity
        int maxMKCapacity = FleetshardUtils.getClusterCapacityFromMKAgent(oc, mkType);
        log.debug("maximal capacity '{}'", maxMKCapacity);
        int freeMKCapacity = FleetshardUtils.getRemainingCapacityMKAgent(oc, mkType);
        log.debug("free capacity '{}'", freeMKCapacity);
        int consumedCapacity = maxMKCapacity - freeMKCapacity;
        log.info("consumed capacity '{}'", consumedCapacity);


        int consumedStreamingUnit = FleetshardUtils.getStreamingUnitCountOfExistingGivenManagedKafkaCRType(oc, mkType);
        log.info("reportedly consumed streaming units '{}'", consumedStreamingUnit);

        // if capacity matches streaming units all is reported correctly
        if (consumedStreamingUnit == consumedCapacity) {
            log.info("consumed capacity matches consumed streaming unit");
            return;
        }

        // otherwise consider following scenarios and wait for some time to capacity to be updated accordingly
        // if there is activity in cluster (i.e., kafka instances being deleted/created) acting against expected change (in capacity) skip test

        Set<String> originalMkNames = FleetshardUtils.listManagedKafka(oc, mkType).stream()
                        .map(e -> e.getMetadata().getName())
                        .filter(e -> e.equals(FleetshardUtils.getReservedDeploymentName(mkType)))
                        .collect(Collectors.toSet());

        if (consumedCapacity < consumedStreamingUnit) {
            log.info("consumed capacity is lower than consumed streaming units, waiting for consumed capacity to increase");

            TestUtils.waitFor(
                "update reported remaining capacity, consumed capacity is to be increased",
                Duration.ofSeconds(10),
                Duration.ofMinutes(6),
                ready -> {
                    // skip if meanwhile any of originally existing kafka instances was deleted.
                    Set<String> mkNamesCurrent = FleetshardUtils.listManagedKafka(oc, mkType).stream()
                        .map(e -> e.getMetadata().getName())
                        .filter(e -> e.equals(FleetshardUtils.getReservedDeploymentName(mkType)))
                        .collect(Collectors.toSet());

                    // keep only names which were present originally, i.e. list contains names of MKs which were deleted
                    Set<String> onlyInstancesBefore = originalMkNames.stream().filter(e -> !mkNamesCurrent.contains(e)).collect(Collectors.toSet());
                    log.debug(onlyInstancesBefore);
                    if (onlyInstancesBefore.size() > 0) {
                        throw new SkipException("skipped due to kafka instance being deleted while waiting for free capacity going down");
                    }

                    int newMKFreeCapacity = FleetshardUtils.getRemainingCapacityMKAgent(oc, mkType);
                    // capacity (taken) increased.
                    return   newMKFreeCapacity < freeMKCapacity;
                });
        } else {
            log.info("less streaming unit are consumed than consumed, waiting for consumed capacity to decrease");
            TestUtils.waitFor(
                "update reported remaining capacity, consumed capacity is to be increased",
                Duration.ofSeconds(10),
                Duration.ofMinutes(6),
                ready -> {
                    // skip if some new kafka is created.
                    Set<String> mkNamesCurrent = FleetshardUtils.listManagedKafka(oc, mkType).stream()
                        .map(e -> e.getMetadata().getName())
                        .filter(e -> e.equals(FleetshardUtils.getReservedDeploymentName(mkType)))
                        .collect(Collectors.toSet());

                    // keep only names which were present originally, i.e. list contains names of MKs which were deleted
                    Set<String> onlyInstancesAfter = mkNamesCurrent.stream().filter(e -> !originalMkNames.contains(e)).collect(Collectors.toSet());
                    log.debug(onlyInstancesAfter);
                    if (onlyInstancesAfter.size() > 0) {
                        throw new SkipException("skipped due to kafka instance being created while waiting for free capacity going up");
                    }

                    int newMKFreeCapacity = FleetshardUtils.getRemainingCapacityMKAgent(oc, mkType);
                    // capacity (free) increased.
                    return   newMKFreeCapacity > freeMKCapacity;
                });
        }
    }

}
