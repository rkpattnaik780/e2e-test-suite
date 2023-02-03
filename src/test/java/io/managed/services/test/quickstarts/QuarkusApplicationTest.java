package io.managed.services.test.quickstarts;

import com.openshift.cloud.api.kas.auth.models.NewTopicInput;
import com.openshift.cloud.api.kas.auth.models.TopicSettings;
import com.openshift.cloud.api.kas.models.KafkaRequest;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.utils.Serialization;
import io.fabric8.openshift.api.model.Route;
import io.fabric8.openshift.client.DefaultOpenShiftClient;
import io.fabric8.openshift.client.OpenShiftClient;
import io.managed.services.test.Environment;
import io.managed.services.test.IsReady;
import io.managed.services.test.TestBase;
import io.managed.services.test.TestUtils;
import io.managed.services.test.WriteStreamConsumer;
import io.managed.services.test.cli.CLI;
import io.managed.services.test.cli.CLIDownloader;
import io.managed.services.test.cli.CLIUtils;
import io.managed.services.test.client.ApplicationServicesApi;
import io.managed.services.test.client.kafkainstance.KafkaInstanceApiUtils;
import io.managed.services.test.client.kafkamgmt.KafkaMgmtApi;
import io.managed.services.test.client.kafkamgmt.KafkaMgmtApiUtils;
import io.managed.services.test.client.oauth.KeycloakUser;
import io.managed.services.test.client.sample.QuarkusSample;
import io.managed.services.test.client.securitymgmt.SecurityMgmtApi;
import io.managed.services.test.framework.LogCollector;
import io.managed.services.test.operator.OperatorUtils;
import io.managed.services.test.operator.ServiceBinding;
import io.managed.services.test.operator.ServiceBindingApplication;
import io.managed.services.test.operator.ServiceBindingSpec;
import io.managed.services.test.operator.ServiceBindingSpecService;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.Json;
import io.vertx.core.streams.WriteStream;
import lombok.SneakyThrows;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.javatuples.Pair;
import org.testng.ITestContext;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import io.managed.services.test.client.oauth.KeycloakLoginSession;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import static io.managed.services.test.TestUtils.assumeTeardown;
import static io.managed.services.test.TestUtils.bwait;
import static io.managed.services.test.TestUtils.decodeBase64;
import static io.managed.services.test.TestUtils.message;
import static io.managed.services.test.TestUtils.waitFor;
import static io.vertx.core.Future.failedFuture;
import static io.vertx.core.Future.succeededFuture;
import static java.time.Duration.ofMinutes;
import static java.time.Duration.ofSeconds;
import static org.testng.Assert.assertNotNull;

/**
 * This tests aims to cover the service discovery quickstart[1] by creating a Kafka Instance, deploy
 * the Quarkus Application on Openshift and use the RHOAS Operator to bind the Instance to the App.
 * <p>
 * 1. https://github.com/redhat-developer/app-services-guides/tree/main/service-discovery
 * <p>
 * <b>Requires:</b>
 * <ul>
 *     <li> PRIMARY_USERNAME
 *     <li> PRIMARY_PASSWORD
 *     <li> DEV_CLUSTER_SERVER
 *     <li> DEV_CLUSTER_TOKEN
 * </ul>
 */
public class QuarkusApplicationTest extends TestBase {
    private static final Logger LOGGER = LogManager.getLogger(QuarkusApplicationTest.class);

    // NOTE: Some names are hard coded because generated from CLI or hard coded in the yaml files

    private static final String KAFKA_INSTANCE_NAME = "mk-e2e-quarkus-" + Environment.LAUNCH_KEY;
    private static final String TOPIC_NAME = "prices";

    // this name is decided from the cli
    private static final String ACCESS_TOKEN_SECRET_NAME = "rh-cloud-services-accesstoken";
    private static final String SERVICE_ACCOUNT_SECRET_NAME = "rh-cloud-services-service-account";

    private static final String SERVICE_BINDING_NAME = "rhoas-kafka-quickstart-bind";

    private static final String APP_YAML_PATH = "quarkus/rhoas-kafka-quickstart-example.yml";
    private static final String APP_SERVICE_NAME = "rhoas-kafka-quickstart-example";
    private static final String APP_DEPLOYMENT_NAME = "rhoas-kafka-quickstart-example";
    private static final String APP_ROUTE_NAME = "rhoas-kafka-quickstart-example";

    private static final String TEST_LOGS_PATH = Environment.LOG_DIR.resolve("test-logs").toString();

    private final Vertx vertx = Vertx.vertx();

    private CLI cli;
    private KeycloakUser user;
    private KafkaMgmtApi kafkaMgmtApi;
    private SecurityMgmtApi securityMgmtApi;
    private OpenShiftClient oc;
    private KafkaRequest kafka;
    private Route route;

    @BeforeClass
    @SneakyThrows
    public void bootstrap() {
        LOGGER.debug("assert necessary environment variables");
        assertNotNull(Environment.PRIMARY_USERNAME, "the PRIMARY_USERNAME env is null");
        assertNotNull(Environment.PRIMARY_PASSWORD, "the PRIMARY_PASSWORD env is null");
        assertNotNull(Environment.DEV_CLUSTER_SERVER, "the DEV_CLUSTER_SERVER env is null");
        assertNotNull(Environment.DEV_CLUSTER_TOKEN, "the DEV_CLUSTER_TOKEN env is null");

        // OC
        LOGGER.info("initialize openshift client");
        var config = new ConfigBuilder()
            .withMasterUrl(Environment.DEV_CLUSTER_SERVER)
            .withOauthToken(Environment.DEV_CLUSTER_TOKEN)
            .withNamespace(Environment.DEV_CLUSTER_NAMESPACE)
            .withTrustCerts(true)
            .build();
        oc = new DefaultOpenShiftClient(config);

        var offlineToken = Environment.PRIMARY_OFFLINE_TOKEN;
        var auth = new KeycloakLoginSession(vertx, Environment.PRIMARY_USERNAME, Environment.PRIMARY_PASSWORD);

        // CLI
        var downloader = CLIDownloader.defaultDownloader();
        LOGGER.info("download cli");
        var cliBinary = downloader.downloadCLIInTempDir();
        LOGGER.info("cli downloaded successfully to: {}", cliBinary);

        LOGGER.info("login to RHOAS CLI (insecure) with authorization based on logging to the console");
        cli = new CLI(cliBinary);
        // insecure login
        CLIUtils.login(vertx, cli, auth, false).get();

        // APIs
        LOGGER.info("initialize kafka and security mgmt apis");
        var apis = new ApplicationServicesApi(Environment.OPENSHIFT_API_URI, offlineToken);
        kafkaMgmtApi = apis.kafkaMgmt();
        securityMgmtApi = apis.securityMgmt();

        // Kafka Instance
        LOGGER.info("create kafka instance '{}'", KAFKA_INSTANCE_NAME);
        kafka = KafkaMgmtApiUtils.applyKafkaInstance(kafkaMgmtApi, KAFKA_INSTANCE_NAME);

        // Topic
        LOGGER.info("create topic '{}'", TOPIC_NAME);
        var kafkaInstanceApi = KafkaInstanceApiUtils.kafkaInstanceApi(kafka, offlineToken);

        var topicPayload = new NewTopicInput()
            .name(TOPIC_NAME)
            .settings(new TopicSettings().numPartitions(1));
        KafkaInstanceApiUtils.applyTopic(kafkaInstanceApi, topicPayload);

        try {
            OperatorUtils.patchTheOperatorCloudServiceAPIEnv(oc);
        } catch (Throwable t) {
            LOGGER.error("failed to patch the CLOUD_SERVICES_API env:", t);
        }
    }

    private void collectQuarkusAppLogs(ITestContext context) throws IOException {

        // save Deployment
        var d = oc.apps().deployments().withName(APP_DEPLOYMENT_NAME).get();
        LogCollector.saveObject(TestUtils.getLogPath(TEST_LOGS_PATH, context), d);

        // save Deployment logs
        LogCollector.saveDeploymentLog(
            TestUtils.getLogPath(TEST_LOGS_PATH, context),
            oc, Environment.DEV_CLUSTER_NAMESPACE, APP_DEPLOYMENT_NAME);
    }

    private void collectKafkaConnection(ITestContext context) throws IOException {
        // save KafkaConnection CR before delete it
        var c = OperatorUtils.kafkaConnection(oc).withName(KAFKA_INSTANCE_NAME).get();
        LogCollector.saveObject(TestUtils.getLogPath(TEST_LOGS_PATH, context), c);
    }

    private void collectServiceBinding(ITestContext context) throws IOException {
        // save KafkaConnection CR before delete it
        var b = OperatorUtils.serviceBinding(oc).withName(SERVICE_BINDING_NAME).get();
        LogCollector.saveObject(TestUtils.getLogPath(TEST_LOGS_PATH, context), b);
    }

    private void cleanAccessTokenSecret() {
        Secret s = oc.secrets().withName(ACCESS_TOKEN_SECRET_NAME).get();
        if (s != null) {
            LOGGER.info("clean secret: {}", s.getMetadata().getName());
            oc.secrets().delete(s);
        }
    }

    private void cleanKafkaConnection() {
        var c = OperatorUtils.kafkaConnection(oc).withName(KAFKA_INSTANCE_NAME).get();
        if (c != null) {
            LOGGER.info("clean ManagedKafkaConnection: {}", c.getMetadata().getName());
            OperatorUtils.kafkaConnection(oc).delete(c);
        }
    }

    private void cleanServiceBinding() {
        var b = OperatorUtils.serviceBinding(oc).withName(SERVICE_BINDING_NAME).get();
        if (b != null) {
            LOGGER.info("clean ServiceBinding: {}", b.getMetadata().getName());
            OperatorUtils.serviceBinding(oc).delete(b);
        }
    }

    private void cleanServiceBindingSecrets() {
        for (var secret : oc.secrets().list().getItems()) {
            if (!secret.getMetadata().getName().startsWith(SERVICE_BINDING_NAME)) {
                continue;
            }

            LOGGER.info("clan secret: {}", secret.getMetadata().getName());
            oc.secrets().delete(secret);
        }
    }

    private void cleanQuarkusSampleApp() {
        oc.apps().deployments().withName(APP_DEPLOYMENT_NAME).delete();
        oc.services().withName(APP_SERVICE_NAME).delete();
        oc.routes().withName(APP_ROUTE_NAME).delete();
    }

    @SneakyThrows
    private void cleanServiceAccount() {
        var secret = oc.secrets().withName(SERVICE_ACCOUNT_SECRET_NAME).get();
        if (secret == null) {
            throw new Error(message("failed to find secret with name: {}", SERVICE_ACCOUNT_SECRET_NAME));
        }

        LOGGER.info("clean service account secret: {}", secret.getMetadata().getName());
        var encodedClientID = secret.getData().get("client-id");
        if (encodedClientID == null) {
            throw new Error(message("client-id data not found in secret: {}", secret));
        }
        var secretClientID = decodeBase64(encodedClientID);

        var accounts = securityMgmtApi.getServiceAccounts();
        var accountOptional = accounts.getItems().stream()
            .filter(a -> secretClientID.equals(a.getClientId()))
            .findAny();

        if (accountOptional.isPresent()) {
            var account = accountOptional.get();

            // delete the service account if founded
            LOGGER.info("delete service account '{}' with id: {}", account.getName(), account.getId());
            securityMgmtApi.deleteServiceAccountById(account.getId());
        } else {
            LOGGER.error("failed to find service account with client-id: {}", secretClientID);
        }

        // delete the secret only after deleting the service account
        LOGGER.info("delete secret: {}", secret.getMetadata().getName());
        oc.secrets().delete(secret);
    }

    private Future<Void> cleanCLI() {
        if (cli != null) {
            LOGGER.info("logout from cli");
            cli.logout();

            LOGGER.info("clean workdir: {}", cli.getWorkdir());
            return vertx.fileSystem().deleteRecursive(cli.getWorkdir(), true);
        }
        return Future.succeededFuture();
    }

    @AfterClass(alwaysRun = true)
    public void teardown(ITestContext context) throws Throwable {
        assumeTeardown();

        try {
            collectQuarkusAppLogs(context);
        } catch (Exception e) {
            LOGGER.error("collectQuarkusAppLogs error: ", e);
        }
        try {
            collectKafkaConnection(context);
        } catch (Exception e) {
            LOGGER.error("collectKafkaConnection error: ", e);
        }
        try {
            collectServiceBinding(context);
        } catch (Exception e) {
            LOGGER.error("collectServiceBinding error: ", e);
        }

        try {
            cleanAccessTokenSecret();
        } catch (Exception e) {
            LOGGER.error("cleanAccessTokenSecret error: ", e);
        }

        try {
            cleanKafkaConnection();
        } catch (Exception e) {
            LOGGER.error("cleanKafkaConnection error: ", e);
        }

        try {
            cleanServiceBinding();
        } catch (Exception e) {
            LOGGER.error("cleanServiceBinding error: ", e);
        }

        try {
            cleanServiceBindingSecrets();
        } catch (Exception e) {
            LOGGER.error("cleanServiceBindingSecrets error: ", e);
        }

        try {
            cleanQuarkusSampleApp();
        } catch (Exception e) {
            LOGGER.error("cleanQuarkusSampleApp error: ", e);
        }

        try {
            cleanServiceAccount();
        } catch (Throwable e) {
            LOGGER.error("cleanServiceAccount error: ", e);
        }

        try {
            KafkaMgmtApiUtils.cleanKafkaInstance(kafkaMgmtApi, KAFKA_INSTANCE_NAME);
        } catch (Throwable e) {
            LOGGER.error("cleanKafkaInstance error: ", e);
        }

        try {
            bwait(cleanCLI());
        } catch (Throwable e) {
            LOGGER.error("cleanCLI error: ", e);
        }

        bwait(vertx.close());
    }

    @Test
    public void testCLIConnectCluster() throws Throwable {
        cleanAccessTokenSecret();
        cleanKafkaConnection();

        var kubeConfig = CLIUtils.kubeConfig(
            Environment.DEV_CLUSTER_SERVER,
            Environment.DEV_CLUSTER_TOKEN,
            Environment.DEV_CLUSTER_NAMESPACE);

        var config = Serialization.asYaml(kubeConfig);

        var kubeconfgipath = cli.getWorkdir() + "/kubeconfig";
        bwait(vertx.fileSystem().writeFile(kubeconfgipath, Buffer.buffer(config)));

        LOGGER.info("cli use kafka instance: {}", kafka.getId());
        cli.useKafka(kafka.getId());

        LOGGER.info("cli cluster connect using kubeconfig: {}", kubeconfgipath);
        cli.connectCluster(user.getRefreshToken(), kubeconfgipath, "kafka");
    }

    @Test(dependsOnMethods = "testCLIConnectCluster")
    @SneakyThrows
    public void testCLIGrantAccess() {

        LOGGER.info("get service account secret: {}", SERVICE_ACCOUNT_SECRET_NAME);
        var secret = oc.secrets().withName(SERVICE_ACCOUNT_SECRET_NAME).get();
        if (secret == null) {
            throw new Error(message("failed to find secret with name: {}", SERVICE_ACCOUNT_SECRET_NAME));
        }

        LOGGER.info("get service account client-id");
        var encodedClientID = secret.getData().get("client-id");
        if (encodedClientID == null) {
            throw new Error(message("client-id data not found in secret: {}", secret));
        }
        var clientID = decodeBase64(encodedClientID);

        LOGGER.info("service account client-id is: {}", clientID);

        LOGGER.info("grant consume and produce access for all topics and groups to the service account: {}", clientID);
        cli.grantProducerAndConsumerAccess(clientID, "all", "all");
    }

    @Test(dependsOnMethods = "testCLIGrantAccess")
    public void testDeployQuarkusApplication() {

        // TODO: Deploy the application from https://raw.githubusercontent.com/redhat-developer/app-services-guides/main/code-examples/quarkus-kafka-quickstart/.kubernetes/kubernetes.yml

        LOGGER.info("deploy the rhoas-kafka-quickstart-example app");
        var resource = QuarkusApplicationTest.class.getClassLoader().getResourceAsStream(APP_YAML_PATH);
        oc.resourceList(oc.load(resource).get()).createOrReplace();

        route = oc.routes().withName(APP_ROUTE_NAME).get();
        LOGGER.info("app deployed to: {}", route.getSpec().getHost());
    }

    @Test(dependsOnMethods = "testDeployQuarkusApplication")
    public void testCreateServiceBinding() throws Throwable {
        cleanServiceBinding();

        // TODO: Bind the Kafka instance to the application using `rhoas cluster bind`

        var application = ServiceBindingApplication.builder()
            .group("apps")
            .name(APP_DEPLOYMENT_NAME)
            .resource("deployments")
            .version("v1")
            .build();

        var service = ServiceBindingSpecService.builder()
            .group("rhoas.redhat.com")
            .version("v1alpha1")
            .kind("KafkaConnection")
            .name(KAFKA_INSTANCE_NAME)
            .build();

        var spec = ServiceBindingSpec.builder()
            .application(application)
            .bindAsFiles(true)
            .services(List.of(service))
            .build();

        var sb = new ServiceBinding();
        sb.setMetadata(new ObjectMeta());
        sb.getMetadata().setName(SERVICE_BINDING_NAME);
        sb.setSpec(spec);

        IsReady<Object> serviceBindingIsDeleted = last -> {
            var b = OperatorUtils.serviceBinding(oc).withName(SERVICE_BINDING_NAME).get();

            if (last) {
                LOGGER.warn("last service binding is: {}", Json.encode(b));
            }
            return succeededFuture(Pair.with(b == null, null));
        };

        IsReady<ServiceBinding> serviceBindingIsReady = last -> {
            var b = OperatorUtils.serviceBinding(oc).withName(SERVICE_BINDING_NAME).get();

            if (b == null) {
                return failedFuture(message("the service binding CR doesn't exists anymore: {}", SERVICE_BINDING_NAME));
            }
            if (last) {
                LOGGER.warn("last service binding is: {}", Json.encode(b));
            }

            var status = b.getStatus();
            if (status == null) {
                return succeededFuture(Pair.with(false, null));
            }

            var isReady = status.getConditions().stream()
                .filter(c -> c.getType().equals("Ready"))
                .findAny()
                .map(c -> c.getStatus().equals("True"))
                .orElse(false);
            return succeededFuture(Pair.with(isReady, b));
        };

        // wait for the service binding to be gone
        bwait(waitFor(vertx, "service binding to be deleted", ofSeconds(3), ofMinutes(1), serviceBindingIsDeleted));

        LOGGER.info("create service binding: {}", Json.encode(sb));
        OperatorUtils.serviceBinding(oc).create(sb);

        bwait(waitFor(vertx, "service binding to be ready", ofSeconds(3), ofMinutes(1), serviceBindingIsReady));
    }

    @Test(dependsOnMethods = "testCreateServiceBinding")
    public void testQuarkusApplication() throws Throwable {

        var endpoint = String.format("http://%s", route.getSpec().getHost());
        var client = new QuarkusSample(vertx, endpoint);

        LOGGER.info("start streaming prices from: {}", endpoint);

        // The /prices/stream endpoint returns a new price every 5s if the
        // quarkus app can connect successfully to the kafka instance
        //
        // The test will wait until it receive 6 price consecutively,
        // and then it will complete

        var reg = Pattern.compile("^data: (?<data>\\d+(\\.\\d+)?)$");

        List<String> prices = new ArrayList<>();
        WriteStream<Buffer> wsc = WriteStreamConsumer.create(buffer -> {

            var s = buffer.toString();
            for (var line : s.split("\\r?\\n")) {
                var match = reg.matcher(line);
                if (match.matches()) {

                    var price = match.group("data");
                    LOGGER.info("match data: {}", price);

                    // add the received price to the list of prices
                    // and complete the request once we have 6 prices (~30s) by return an error
                    prices.add(price);
                    if (prices.size() > 6) {
                        return Future.failedFuture("complete");
                    }
                }
            }

            return Future.succeededFuture();
        });

        IsReady<Object> complete = last -> client.streamPrices(wsc)
            .recover(t -> {
                LOGGER.warn("ignore error: {}", t.getMessage());
                return succeededFuture();
            })
            .map(___ -> {
                if (prices.size() > 6) {
                    return Pair.with(true, null);
                }

                // clean the data list between each failure
                prices.clear();

                return Pair.with(false, null);
            });
        bwait(waitFor(vertx, "retry data", ofSeconds(1), ofMinutes(3), complete));
    }
}
