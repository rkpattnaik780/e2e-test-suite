package io.managed.services.test.quickstarts.steps;

import io.managed.services.test.Environment;
import io.managed.services.test.quickstarts.contexts.KafkaInstanceContext;
import io.managed.services.test.quickstarts.contexts.ServiceAccountContext;
import lombok.extern.log4j.Log4j2;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.api.errors.JGitInternalException;
import io.cucumber.java.en.When;
import io.cucumber.java.en.Then;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import static org.testng.Assert.assertTrue;

@Log4j2
public class JavaClientSteps {

    private File repository;

    private static final String TMPDIR = "quickstart-repo";

    private final KafkaInstanceContext kafkaInstanceContext;
    private final ServiceAccountContext serviceAccountContext;

    public static String stdOutput = null;

    private Map<String, String> envsMap = new HashMap<>();

    public JavaClientSteps(KafkaInstanceContext kafkaInstanceContext, ServiceAccountContext serviceAccountContext) {
        this.kafkaInstanceContext = kafkaInstanceContext;
        this.serviceAccountContext = serviceAccountContext;
    }

    // TODO: consider using github context
    // temporary usage to fetch examples from a different repository
    @When("you clone the {word} repository from GitHub examples for Java")
    public void you_clone_the_app_services_guides_repository_from_git_hub(String repository) throws IOException {
        log.info("download git repository java");

        var appRepositoryTempWorkDir = Files.createTempDirectory(TMPDIR);
        log.info("created tmp application directory: {}", appRepositoryTempWorkDir.toString());
        appRepositoryTempWorkDir.toFile().deleteOnExit();

        try {
            // TODO: Change it to "https://github.com/redhat-developer/app-services-guides"
            String repositoryLink = "https://github.com/rkpattnaik780/rhosak_example_codes";
            this.repository = new File(appRepositoryTempWorkDir.toString() + "/" + repository);
            Git.cloneRepository().setURI(repositoryLink).setDirectory(this.repository).call();
            log.info("Successfully downloaded the repository!");
        } catch (GitAPIException | JGitInternalException e) {
            log.error(e.getMessage());
            e.printStackTrace();
        }
    }

    @Then("the {word} example repository for java is available locally")
    public void the_app_services_guides_repository_is_available_locally(String repository) {
        log.info("verify existence of repository");
        assertTrue(this.repository.exists(), "repository does not exist");
    }

    @When("you set the Kafka instance bootstrap server endpoint, service account credentials, and OAUTHBEARER token endpoint as environment variables for Java")
    public void you_set_the_kafka_instance_bootstrap_server_endpoint_service_account_credentials_and_oauthbearer_token_endpoint_as_environment_variables_for_java() {
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

    @Then("you run Java client example producer")
    public void you_run_java_client_example_producer() throws IOException {

        log.info("package run Java client example producer application");

        // set path to root of quickstart
        String quickstartRoot = this.repository.getAbsolutePath() + "/kafka-java-maven";

        ProcessBuilder builder = new ProcessBuilder("mvn", "compile", "exec:java", "-Pproducer");
        builder.directory(Paths.get(quickstartRoot).toFile());
        Map<String, String> processEnvsMap2 = builder.environment();
        envsMap.entrySet().forEach(entry -> {
            processEnvsMap2.put(entry.getKey(), entry.getValue());
            log.info("exporting env. variable:" + entry.getKey() + "=" + entry.getValue());
        });
        builder.redirectErrorStream(true); //merge input and error streams

        Process process = builder.start();
        BufferedReader stdInput = new BufferedReader(new InputStreamReader(process.getInputStream()));
        while ((stdOutput = stdInput.readLine()) != null) {
            System.out.println(stdOutput);
        }
    }

    @When("you run Java client example consumer")
    public void you_run_java_client_example_consumer() throws Exception {

        log.info("package run Java client example consumer application");

        // set path to root of quickstart
        String quickstartRoot = this.repository.getAbsolutePath() + "/kafka-java-maven";

        ProcessBuilder builder = new ProcessBuilder("mvn", "compile", "exec:java", "-Pconsumer");
        builder.directory(Paths.get(quickstartRoot).toFile());
        Map<String, String> processEnvsMap2 = builder.environment();
        envsMap.entrySet().forEach(entry -> {
            processEnvsMap2.put(entry.getKey(), entry.getValue());
            log.info("exporting env. variable:" + entry.getKey() + "=" + entry.getValue());
        });
        builder.redirectErrorStream(true); //merge input and error streams

        Process process = builder.start();
        BufferedReader stdInput = new BufferedReader(new InputStreamReader(process.getInputStream()));
        String output = "";
        while ((output = stdInput.readLine()) != null) {
            stdOutput += output;
            System.out.println(output);
        }
    }

    @Then("Java client consumer should print proper message")
    public void javaClientConsumerShouldPrintProperMessage() throws Exception {

        if (!stdOutput.contains("Test")) {
            throw new IOException("Not able to find the produced message");
        }
    }
}
