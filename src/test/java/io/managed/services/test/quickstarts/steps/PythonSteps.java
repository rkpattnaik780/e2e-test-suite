package io.managed.services.test.quickstarts.steps;

import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import io.managed.services.test.Environment;
import io.managed.services.test.quickstarts.contexts.KafkaInstanceContext;
import io.managed.services.test.quickstarts.contexts.ServiceAccountContext;
import lombok.extern.log4j.Log4j2;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.api.errors.JGitInternalException;

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
public class PythonSteps {

    private File repository;

    private static final String TMPDIR = "quickstart-repo";

    private final KafkaInstanceContext kafkaInstanceContext;
    private final ServiceAccountContext serviceAccountContext;

    public static String stdOutput = null;

    private Map<String, String> envsMap = new HashMap<>();

    public PythonSteps(KafkaInstanceContext kafkaInstanceContext, ServiceAccountContext serviceAccountContext) {
        this.kafkaInstanceContext = kafkaInstanceContext;
        this.serviceAccountContext = serviceAccountContext;
    }

    // temporary usage to fetch examples from a different repository
    @When("you clone the {word} repository from GitHub examples")
    public void you_clone_the_app_services_guides_repository_from_git_hub(String repository) throws IOException {
        log.info("download git repository");

        var appRepositoryTempWorkDir = Files.createTempDirectory(TMPDIR);
        log.info("created tmp application directory: {}", appRepositoryTempWorkDir.toString());
        appRepositoryTempWorkDir.toFile().deleteOnExit();

        try {
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
    }

    @When("you set the Kafka instance bootstrap server endpoint, service account credentials, and OAUTHBEARER token endpoint as environment variables for Python app")
    public void you_set_the_kafka_instance_bootstrap_server_endpoint_service_account_credentials_and_oauthbearer_token_endpoint_as_environment_variables_python() {
        // Write code here that turns the phrase above into concrete actions
        log.info("setting up map of all environment variables for python application");
        envsMap.put("KAFKA_HOST", kafkaInstanceContext.requireKafkaInstance().getBootstrapServerHost());
        envsMap.put("RHOAS_SERVICE_ACCOUNT_CLIENT_ID", serviceAccountContext.requireServiceAccount().getClientId());
        envsMap.put("RHOAS_SERVICE_ACCOUNT_CLIENT_SECRET",
                serviceAccountContext.requireServiceAccount().getClientSecret());
        envsMap.put("RHOAS_SERVICE_ACCOUNT_OAUTH_TOKEN_URL", String.format("%s/auth/realms/%s/protocol/openid-connect/token", Environment.REDHAT_SSO_URI,
                Environment.REDHAT_SSO_REALM));
        log.debug(envsMap);
    }

    @When("you run Python example producer")
    public void you_run_python_example_producer() throws IOException {

        log.info("package run Python example producer application");

        // set path to root of quickstart
        String quickstartRoot = this.repository.getAbsolutePath() + "/python_example";

        ProcessBuilder builder = new ProcessBuilder("python3", "producer.py");
        builder.directory(Paths.get(quickstartRoot).toFile());
        Map<String, String> processEnvsMap2 = builder.environment();
        envsMap.entrySet().forEach(entry -> {
            processEnvsMap2.put(entry.getKey(), entry.getValue());
            log.info("exporting env. variable:" + entry.getKey() + "=" + entry.getValue());
        });
        builder.redirectErrorStream(true); //merge input and error streams
        Process process = builder.start();

        BufferedReader stdInput = new BufferedReader(new InputStreamReader(process.getInputStream()));
        String s = null;
        while ((s = stdInput.readLine()) != null) {
            System.out.println(s);
        }

    }

    @When("you run Python client consumer")
    public void youRunPythonClientConsumer() throws IOException {

        log.info("package run Python example consumer application");

        // set path to root of quickstart
        String quickstartRoot = this.repository.getAbsolutePath() + "/python_example";

        // build process to package all dependencies
        ProcessBuilder builder = new ProcessBuilder("python3", "consumer.py");
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

    @Then("Python example consumer should print proper message")
    public void pythonExampleConsumerShouldPrintProperMessage() throws Exception {
        if (!stdOutput.contains("Shore")) {
            throw new IOException("Not able to find the produced message");
        }
    }
}
