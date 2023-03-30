package io.managed.services.test.cli;

import io.managed.services.test.Environment;
import io.managed.services.test.RetryUtils;
import io.managed.services.test.ThrowingSupplier;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.time.Duration.ofMinutes;

public class KafkaScripts {
    private static final Logger LOGGER = LogManager.getLogger(KafkaScripts.class);

    private static final Duration DEFAULT_TIMEOUT = ofMinutes(1);

    private String binariesWorkdir;
    private Path rootWorkDir;

    private final String kafkaVersion = "3.2.3";
    private final String scalaVersion = "2.13";
    private final String kafkaResource = "kafka_" + scalaVersion + "-" + kafkaVersion;


    public KafkaScripts(Path rootWorkDirectory) {
        this.rootWorkDir = rootWorkDirectory;
    }

    // root work dir is returned in order to clean whole temporary dir with all nested content.
    public Path getRootWorkDir() {
        return this.rootWorkDir;
    }

    private ProcessBuilder builder(List<String> command) {

        // retrieve first item which represents script to be executed
        var scriptName = command.get(0);
        var platform = Environment.CLI_PLATFORM;

        var cmd = new ArrayList<String>(command);
        // not tested on widows but may work.
        if (Platform.WINDOWS.toString().equals(platform)) {
            cmd.set(0, ".\\" + scriptName + ".bat");
        } else {
            cmd.set(0, "./" + scriptName + ".sh");
        }

        return new ProcessBuilder(cmd).directory(new File(binariesWorkdir));
    }

    private AsyncProcess exec(String... command) throws ProcessException {
        return exec(List.of(command));
    }

    private AsyncProcess exec(List<String> command) throws ProcessException {
        return execAsync(command).sync(DEFAULT_TIMEOUT);
    }

    private AsyncProcess execAsync(String... command) {
        return execAsync(List.of(command));
    }

    private AsyncProcess execAsync(List<String> command) {
        try {
            return new AsyncProcess(builder(command).start());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void downloadAndExtractKafkaScripts() throws Throwable {

        String kafkaURLString = String.format("https://dlcdn.apache.org/kafka/%s/%s.tgz", kafkaVersion, kafkaResource);
        LOGGER.info("downloading kafka scripts from url: {}", kafkaURLString);

        // alternative download for exec("wget", kafkaURLString, "-P", ".", "--quiet")

        // attempt to download kafka scripts
        Path source = Paths.get(rootWorkDir.toString(), "kafka.tgz");
        int connectionTimeoutMs = 60_000;
        int readTimeoutMs = 60_000;
        // function to attempt download kafka scripts
        ThrowingSupplier<Void, Throwable> attemptDownloadCli = () -> {
            LOGGER.info("start attempt to download kafka scripts");
            FileUtils.copyURLToFile(new URL(kafkaURLString), source.toFile(), connectionTimeoutMs, readTimeoutMs);
            LOGGER.info("finish attempt to download kafka scripts");
            return null;
        };

        // retry condition on downloading kafka scripts
        Function<Throwable, Boolean> downloadRetryCondition = (Throwable e) -> {
            // if downloading of kafka scripts fails on some sort of unreliable connection repeat several times.
            if (e instanceof ConnectException || e instanceof SocketTimeoutException) {
                LOGGER.warn("connection timeout while downloading kafka scripts: {}", e.toString());
                return true;
            }

            LOGGER.warn(e);
            return  false;
        };

        // download kafka scripts
        RetryUtils.retry(
                1,
                attemptDownloadCli,
                downloadRetryCondition,
                3
        );

        LOGGER.info("extract binaries");

        // destination of decompression
        Path target = rootWorkDir;

        if (Files.notExists(source)) {
            throw new IOException(" The file you want to extract does not exist ");
        }


        try (InputStream fi = Files.newInputStream(source);
             BufferedInputStream bi = new BufferedInputStream(fi);
             GzipCompressorInputStream gzi = new GzipCompressorInputStream(bi);
             TarArchiveInputStream ti = new TarArchiveInputStream(gzi)) {

            ArchiveEntry entry;
            while ((entry = ti.getNextEntry()) != null) {

                // Get the unzip file directory , And determine whether the file is damaged
                Path newPath = zipSlipProtect(entry, target);

                if (entry.isDirectory()) {
                    // Create a directory for extracting files
                    Files.createDirectories(newPath);
                } else {
                    // Verify the existence of the extracted file directory again
                    Path parent = newPath.getParent();
                    if (parent != null) {
                        if (Files.notExists(parent)) {
                            Files.createDirectories(parent);
                        }
                    }
                    //  Input the extracted file into TarArchiveInputStream, Output to disk newPath Catalog
                    Files.copy(ti, newPath, StandardCopyOption.REPLACE_EXISTING);

                }
            }
        }

        // update command path to the kafka binaries. If windows /bin/windows otherwise /bin
        var platform = Environment.CLI_PLATFORM;

        if (Platform.WINDOWS.toString().equals(platform)) {
            LOGGER.info("move to /bin/windows as bin directory for windows platform");
            this.binariesWorkdir = Paths.get(rootWorkDir.toString(), kafkaResource, "bin", "windows").toString();
        } else {
            LOGGER.info("move to /bin as bin directory for none windows platform");
            this.binariesWorkdir = Paths.get(rootWorkDir.toString(), kafkaResource, "bin").toString();
        }

        // after unzip in java all rights to execute are forbidden.
        // Because scripts that we will use depends on others we will allow execution of all
        try (Stream<Path> paths = Files.walk(Paths.get(this.binariesWorkdir))) {
            paths
                    .filter(Files::isRegularFile)
                    .forEach(f -> f.toFile().setExecutable(true, false));
        }
    }

    // Determine whether the compressed file is damaged , And return to the unzipped directory of the file
    private Path zipSlipProtect(ArchiveEntry entry, Path targetDir) throws IOException {
        Path targetDirResolved = targetDir.resolve(entry.getName());
        Path normalizePath = targetDirResolved.normalize();

        if (!normalizePath.startsWith(targetDir)) {
            throw new IOException(" The compressed file has been damaged : " + entry.getName());
        }

        return normalizePath;
    }

    public Path createAndSetUpConfigFile(String content) throws ProcessException, IOException {
        String propertiesFile = "app-services.properties";

        Path pathToNewPropertyFile = Paths.get(rootWorkDir.toString(), kafkaResource, "config", propertiesFile);

        FileWriter fileWriter = new FileWriter(pathToNewPropertyFile.toString());
        PrintWriter printWriter = new PrintWriter(fileWriter);
        printWriter.print(content);
        printWriter.close();

        return pathToNewPropertyFile;
    }

    public String version() throws ProcessException {
        return exec(List.of("kafka-console-consumer", "--version")).stdoutAsString();
    }

    public AsyncProcess kafkaConsoleConsumerAvailable() throws ProcessException {
        return exec(List.of("kafka-console-consumer", "--version"));
    }

    public AsyncProcess kafkaConsoleProducerAvailable() throws ProcessException {
        return exec(List.of("kafka-console-producer", "--version"));
    }

    public AsyncProcess kafkaTopicsAvailable() throws ProcessException {
        return exec(List.of("kafka-topics", "--version"));
    }

    public String createTopic(String topicName, String bootstrapServer, Path propertiesFIle) throws ProcessException {
        return exec(
                "kafka-topics",
                "--create",
                "--topic", topicName,
                "--bootstrap-server", bootstrapServer,
                "--command-config", propertiesFIle.toString()
        ).stdoutAsString();
    }

    public String deleteTopic(String topicName, String bootstrapServer, Path propertiesFIle) throws ProcessException {
        return exec(
                "kafka-topics",
                "--delete",
                "--topic", topicName,
                "--bootstrap-server", bootstrapServer,
                "--command-config", propertiesFIle.toString()
        ).stdoutAsString();
    }

    public AsyncProcess startKafkaConsoleProducer(String topicName, String bootstrapServer, Path propertiesFIle) {

        return execAsync(
                "kafka-console-producer",
                "--topic", topicName,
                "--bootstrap-server", bootstrapServer,
                "--producer.config", propertiesFIle.toString()
        );
    }

    public AsyncProcess startKafkaConsoleConsumer(String topicName, String bootstrapServer, Path propertiesFIle) {

        return execAsync(
                "kafka-console-consumer",
                "--topic", topicName,
                "--bootstrap-server", bootstrapServer,
                "--from-beginning",
                "--consumer.config", propertiesFIle.toString()
        );
    }
}
