package io.managed.services.test.quickstarts.steps;

import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import io.managed.services.test.Environment;
import io.managed.services.test.cli.AsyncProcess;
import io.managed.services.test.cli.CliGenericException;
import io.managed.services.test.cli.ProcessException;
import lombok.extern.log4j.Log4j2;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.api.errors.JGitInternalException;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.testng.Assert.assertTrue;

@Log4j2
public class PythonSteps {

    private File repository;

    private static final String TMPDIR = "quickstart-repo";

    private Map<String, String> envsMap = new HashMap<>();

    private AsyncProcess pythonApplicationProcess;

    @When("you clone the {word} repository from GitHub two")
    public void you_clone_the_app_services_guides_repository_from_git_hub(String repository) throws IOException {
        log.info("download git repository");

        var appRepositoryTempWorkDir = Files.createTempDirectory(TMPDIR);
        log.info("created tmp application directory: {}", appRepositoryTempWorkDir.toString());
        appRepositoryTempWorkDir.toFile().deleteOnExit();

        try {
            String repositoryLink = "https://github.com/rkpattnaik780/rhosak_example_codes";
            //this.repository = new File(AppRepositoryTempWorkDir.toFile());
            this.repository = new File(appRepositoryTempWorkDir.toString() + "/" + repository);
            Git.cloneRepository().setURI(repositoryLink).setDirectory(this.repository).call();
            log.info("Successfully downloaded the repository!");
        } catch (GitAPIException | JGitInternalException e) {
            log.error(e.getMessage());
            e.printStackTrace();
        }
    }

    @Then("the {word} repository is available locally two")
    public void the_app_services_guides_repository_is_available_locally(String repository) {
        log.info("verify existence of repository");
        assertTrue(this.repository.exists(), "repository does not exist");
    }

    @When("you run Python example producer")
    public void you_run_python_example_producer() throws IOException {

        log.info("package run Python example application");

        // set path to root of quickstart
        String quickstartRoot = this.repository.getAbsolutePath();

        log.info("Quick start root is set as:");
        log.info(quickstartRoot);

        // build process to package all dependencies
        ProcessBuilder builder = new ProcessBuilder("python3", "python_example/producer.py");

        builder.redirectErrorStream(true); //merge input and error streams
        Process process = builder.start();

    }

    @When("Python example consumer should print the produced message")
    public void python_example_consumer_should_print_the_produced_message() throws IOException {

        log.info("package run Python example application");

        // set path to root of quickstart
        String quickstartRoot = this.repository.getAbsolutePath();

        log.info("Quick start root is set as:");
        log.info(quickstartRoot);

        // build process to package all dependencies
        ProcessBuilder builder = new ProcessBuilder("python3", "python_example/consumer.py");

        builder.redirectErrorStream(true); //merge input and error streams
        Process process = builder.start();

        BufferedReader stdInput = new BufferedReader(new InputStreamReader(process.getInputStream()));

        String s = null;
        String output = "";
        while ((s = stdInput.readLine()) != null) {
            output += s;
            System.out.println(s);
        }

        if(!output.contains("Shore")) {
            throw new IOException("NOt able to find the produced message");
        }

    }

//    @Then("bla bla vla")
//    public void blaBlaVla() {
//
//        log.info(("dhnsfjjkf dsjjkj"));
//    }
}
