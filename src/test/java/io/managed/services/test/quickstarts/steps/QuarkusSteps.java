package io.managed.services.test.quickstarts.steps;

import io.cucumber.java.After;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import io.managed.services.test.Environment;
import io.managed.services.test.IsReady;
import io.managed.services.test.WriteStreamConsumer;
import io.managed.services.test.cli.AsyncProcess;
import static org.testng.Assert.assertTrue;
import io.managed.services.test.cli.CliGenericException;
import io.managed.services.test.cli.ProcessException;
import io.managed.services.test.client.exception.ApiGenericException;
import io.managed.services.test.client.kafkainstance.KafkaInstanceApiAccessUtils;
import io.managed.services.test.client.sample.QuarkusSample;
import io.managed.services.test.quickstarts.contexts.KafkaInstanceContext;
import io.managed.services.test.quickstarts.contexts.ServiceAccountContext;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.streams.WriteStream;
import lombok.extern.log4j.Log4j2;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.api.errors.JGitInternalException;
import org.javatuples.Pair;

import static io.managed.services.test.TestUtils.assumeTeardown;
import static io.managed.services.test.TestUtils.bwait;
import static io.managed.services.test.TestUtils.waitFor;
import static io.vertx.core.Future.succeededFuture;
import static java.time.Duration.ofMinutes;
import static java.time.Duration.ofSeconds;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

@Log4j2
public class QuarkusSteps {

    // contexts
    private final KafkaInstanceContext kafkaInstanceContext;
    private final ServiceAccountContext serviceAccountContext;

    private final Vertx vertx = Vertx.vertx();

    private static final String TMPDIR = "quickstart-repo";

    private Map<String, String> envsMap = new HashMap<>();
    private File repository;
    private AsyncProcess quarkusApplicationProcess;

    public QuarkusSteps(KafkaInstanceContext kafkaInstanceContext, ServiceAccountContext serviceAccountContext) {
        this.kafkaInstanceContext = kafkaInstanceContext;
        this.serviceAccountContext = serviceAccountContext;
    }

    @When("you clone the {word} repository from GitHub")
    public void you_clone_the_app_services_guides_repository_from_git_hub(String repository) throws IOException {
        log.info("download git repository");

        var appRepositoryTempWorkDir = Files.createTempDirectory(TMPDIR);
        log.info("created tmp application directory: {}", appRepositoryTempWorkDir.toString());
        appRepositoryTempWorkDir.toFile().deleteOnExit();

        try {
            String repositoryLink = "https://github.com/redhat-developer/app-services-guides";
            //this.repository = new File(AppRepositoryTempWorkDir.toFile());
            this.repository = new File(appRepositoryTempWorkDir.toString() + "/" + repository);
            Git.cloneRepository().setURI(repositoryLink).setDirectory(this.repository).call();
            log.info("Successfully downloaded the repository!");
        } catch (GitAPIException | JGitInternalException e) {
            log.error(e.getMessage());
            e.printStackTrace();
        }
    }

    @Then("the {word} repository is available locally")
    public void the_app_services_guides_repository_is_available_locally(String repository) {
        log.info("verify existence of repository");
        assertTrue(this.repository.exists(), "repository does not exist");
    }

    @Given("you have the OAUTHBEARER token endpoint for the Kafka instance")
    public void you_have_the_oauthbearer_token_endpoint_for_the_kafka_instance() {
        log.info(String.format("%s/auth/realms/%s/protocol/openid-connect/token", Environment.REDHAT_SSO_URI,
                Environment.REDHAT_SSO_REALM));
    }

    @When("you set the Kafka instance bootstrap server endpoint, service account credentials, and OAUTHBEARER token endpoint as environment variables")
    public void you_set_the_kafka_instance_bootstrap_server_endpoint_service_account_credentials_and_oauthbearer_token_endpoint_as_environment_variables() {
        // Write code here that turns the phrase above into concrete actions
        log.info("setting up map of all environment variables for quarkus application");
        envsMap.put("KAFKA_HOST", kafkaInstanceContext.requireKafkaInstance().getBootstrapServerHost());
        envsMap.put("RHOAS_SERVICE_ACCOUNT_CLIENT_ID", serviceAccountContext.requireServiceAccount().getClientId());
        envsMap.put("RHOAS_SERVICE_ACCOUNT_CLIENT_SECRET",
                serviceAccountContext.requireServiceAccount().getClientSecret());
        envsMap.put("RHOAS_SERVICE_ACCOUNT_OAUTH_TOKEN_URL", String.format("%s/auth/realms/%s/protocol/openid-connect/token", Environment.REDHAT_SSO_URI,
                Environment.REDHAT_SSO_REALM));
        log.debug(envsMap);
    }

    @Given("you have set the permissions for your service account to produce and consume from topic {word}")
    public void grantAllACLsToReadAndWriteToTopic(String topicName) throws ApiGenericException {
        var kafkaInstanceApi = kafkaInstanceContext.kafkaInstanceApi();
        var serviceAccount = serviceAccountContext.requireServiceAccount();

        var principal = KafkaInstanceApiAccessUtils.toPrincipal(serviceAccount.getClientId());
        log.info("apply acl to create topics for principal '{}'", principal);

        KafkaInstanceApiAccessUtils.createProducerAndConsumerACLs(kafkaInstanceApi, principal);
    }

    // TODO container version works only with packaging uber-jar and later running .jar instead of recommended ./mvnw
    //  quarkus:dev
    @When("you run Quarkus example applications")
    public void you_run_quarkus_example_applications() throws IOException, ProcessException, InterruptedException, CliGenericException {

        log.info("package run quarkus example application 2");

        // set path to root of quickstart
        String quickstartRoot = this.repository.getAbsolutePath() +
                "/code-examples/quarkus-kafka-quickstart";

        // build process to package all dependencies
        ProcessBuilder builder = new ProcessBuilder("./mvnw", "package", "-Dquarkus.package.type=uber-jar",
                "-Dquarkus.profile=dev");
        builder.directory(Paths.get(quickstartRoot).toFile());
        builder.redirectErrorStream(true); //merge input and error streams
        Process process = builder.start();

        // logging of all downloaded of all dependencies
        AtomicBoolean isProcessFinished = new AtomicBoolean(false);
        Thread singleThread = new Thread(() -> {
            //read from the merged stream
            try (BufferedReader reader =
                         new BufferedReader(new InputStreamReader(process.getInputStream()))) {
                String line;
                //read until the stream is exhausted, meaning the process has terminated
                while ((line = reader.readLine()) != null) {
                    log.trace(line); //use the output here
                }
                // make sure process is finished
                isProcessFinished.set(process.waitFor(4, TimeUnit.MINUTES));
            } catch (Exception e) {
                log.error("error while reading output from packaging quarkus application, {}", e.getMessage());
            }
        });
        singleThread.start();
        singleThread.join();
        assertTrue(isProcessFinished.get(), "packaging of quarkus application did not finish within expected time (4 min)");

        //log.info("packaging application");
        //var output =
        //        exec(quickstartRoot, "./mvnw", "package", "-Dquarkus.package.type=uber-jar", "-Dquarkus.profile=dev").stdoutAsString();
        //log.info(output);

        // find path to java executable
        Path javaExecutablePath;
        if (Environment.CLI_PLATFORM.equals("macOS")) {
            javaExecutablePath = Paths.get("/usr/bin");
        } else {
            log.debug("platform used: {}", Environment.CLI_PLATFORM);
            javaExecutablePath = Paths.get("/bin");
        }

        // executed target JAR/
        String executableJar = quickstartRoot +
                "/target/quarkus-kafka-quickstart-1.0-SNAPSHOT-runner.jar";
        ProcessBuilder appProcessBuilder = new ProcessBuilder(List.of("./java",
                "-jar", executableJar));
        appProcessBuilder.directory(Paths.get(javaExecutablePath.toString()).toFile());
        Map<String, String> processEnvsMap2 = appProcessBuilder.environment();
        envsMap.entrySet().forEach(entry -> {
            processEnvsMap2.put(entry.getKey(), entry.getValue());
            log.info("exporting env. variable:" + entry.getKey() + "=" + entry.getValue());
        });
        quarkusApplicationProcess = new AsyncProcess(appProcessBuilder.start());

    }

    @Then("the application is running and the `Last price` is updated at http localhost port 8080 resource prices.html")
    public void theApplicationIsRunningAndTheLastPriceIsUpdatedAtHttpLocalhostPortResourcePricesHtml() throws Throwable {

        var endpoint = "http://localhost:8080";
        var client = new QuarkusSample(vertx, endpoint);

        log.info("start streaming prices from: {}", endpoint);

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
                    log.info("match data: {}", price);

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
                    log.warn("ignore error: {}", t.getMessage());
                    log.info("quarkus application output:\n{}", quarkusApplicationProcess.outputAsString());
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
        bwait(waitFor(vertx, "retry data", ofSeconds(4), ofMinutes(4), complete));
    }


    @After(order = 10210)
    public void cleanQuarkusAppProcess() {

        // nothing to clan
        if (quarkusApplicationProcess == null) return;

        assumeTeardown();

        try {
            quarkusApplicationProcess.destroy();
            log.info("quarkus application output:\n{}", quarkusApplicationProcess.stdoutAsString());
        } catch (Throwable t) {
            log.error("close quarkus application error:", t);
        }

        quarkusApplicationProcess = null;
    }


}
