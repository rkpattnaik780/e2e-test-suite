package io.managed.services.test.quickstarts.steps;

import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import io.managed.services.test.Environment;
import io.managed.services.test.quickstarts.contexts.KafkaInstanceContext;
import io.managed.services.test.quickstarts.contexts.ServiceAccountContext;
import lombok.extern.log4j.Log4j2;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.api.errors.JGitInternalException;
import java.io.InputStreamReader;
import org.eclipse.jgit.api.Git;

import static org.testng.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

@Log4j2
public class SpringBootSteps {

    private File repository;

    private static final String TMPDIR = "quickstart-repo";

    private final KafkaInstanceContext kafkaInstanceContext;
    private final ServiceAccountContext serviceAccountContext;

    public static String stdOutput = null;

    private Map<String, String> envsMap = new HashMap<>();

    public SpringBootSteps(KafkaInstanceContext kafkaInstanceContext, ServiceAccountContext serviceAccountContext) {
        this.kafkaInstanceContext = kafkaInstanceContext;
        this.serviceAccountContext = serviceAccountContext;
    }

    // TODO: consider using github context
    // temporary usage to fetch examples from a different repository
    @When("you clone the {word} repository from GitHub examples")
    public void you_clone_the_app_services_guides_repository_from_git_hub(String repository) throws IOException {
        log.info("download git repository springboot");

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

    @Then("the {word} example repository is available locally")
    public void the_app_services_guides_repository_is_available_locally(String repository) {
        log.info("verify existence of repository");
        assertTrue(this.repository.exists(), "repository does not exist");
        System.out.println();
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

    @Then("you run Springboot client example producer")
    public void youRunSpringbootClientExampleProducer() throws IOException {

        log.info("package run Springboot client example producer application");

        // set path to root of quickstart
        String quickstartRoot = this.repository.getAbsolutePath() + "/kafka-producer-spring";



        // mvn compile exec:java -Dexec.mainClass="com.example.kafkademo.KafkaProducerExample"
        ProcessBuilder builder = new ProcessBuilder("mvn", "compile", "exec:java", "-Pproducer");

        Map<String, String> processEnvsMap2 = builder.environment();
        envsMap.entrySet().forEach(entry -> {
            processEnvsMap2.put(entry.getKey(), entry.getValue());
            log.info("exporting env. variable:" + entry.getKey() + "=" + entry.getValue());
        });
        builder.directory(Paths.get(quickstartRoot).toFile());
        builder.redirectErrorStream(true); //merge input and error streams

        Process process = builder.start();
        BufferedReader stdInput = new BufferedReader(new InputStreamReader(process.getInputStream()));
        while ((stdOutput = stdInput.readLine()) != null) {
            System.out.println(stdOutput);
        }
    }

    @When("you set the Kafka instance bootstrap server endpoint, service account credentials, and OAUTHBEARER token endpoint as environment variables for Spring app")
    public void youSetTheKafkaInstanceBootstrapServerEndpointServiceAccountCredentialsAndOAUTHBEARERTokenEndpointAsEnvironmentVariablesForSpringApp() {

        log.info("setting up map of all environment variables for python application");
        envsMap.put("KAFKA_HOST", kafkaInstanceContext.requireKafkaInstance().getBootstrapServerHost());
        envsMap.put("RHOAS_SERVICE_ACCOUNT_CLIENT_ID", serviceAccountContext.requireServiceAccount().getClientId());
        envsMap.put("RHOAS_SERVICE_ACCOUNT_CLIENT_SECRET",
                serviceAccountContext.requireServiceAccount().getClientSecret());
        envsMap.put("RHOAS_SERVICE_ACCOUNT_OAUTH_TOKEN_URL", String.format("%s/auth/realms/%s/protocol/openid-connect/token", Environment.REDHAT_SSO_URI,
                Environment.REDHAT_SSO_REALM));
        log.debug(envsMap);
    }

    @When("you run Springboot client example consumer")
    public void youRunSpringbootClientExampleConsumer() throws IOException {

        log.info("package run Springboot client example consumer application");

        // set path to root of quickstart
        String quickstartRoot = this.repository.getAbsolutePath() + "/kafka-producer-spring";

        // mvn compile exec:java -Dexec.mainClass="com.example.kafkademo.KafkaProducerExample"
        ProcessBuilder builder = new ProcessBuilder("mvn", "compile", "exec:java", "-Pconsumer");

        Map<String, String> processEnvsMap2 = builder.environment();
        envsMap.entrySet().forEach(entry -> {
            processEnvsMap2.put(entry.getKey(), entry.getValue());
            log.info("exporting env. variable:" + entry.getKey() + "=" + entry.getValue());
        });
        builder.directory(Paths.get(quickstartRoot).toFile());
        builder.redirectErrorStream(true); //merge input and error streams

        Process process = builder.start();
        BufferedReader stdInput = new BufferedReader(new InputStreamReader(process.getInputStream()));
        String output = "";
        while ((output = stdInput.readLine()) != null) {
            stdOutput += output;
        }
    }

    @Then("Springboot consumer should print proper message")
    public void springbootConsumerShouldPrintProperMessage() throws Exception {
        if (!stdOutput.contains("Test")) {
            throw new IOException("Not able to find the produced message");
        }
    }
}
