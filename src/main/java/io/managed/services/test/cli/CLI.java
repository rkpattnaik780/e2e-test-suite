package io.managed.services.test.cli;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.openshift.cloud.api.kas.auth.models.AclBindingListPage;
import com.openshift.cloud.api.kas.auth.models.ConsumerGroup;
import com.openshift.cloud.api.kas.auth.models.ConsumerGroupList;
import com.openshift.cloud.api.kas.auth.models.Topic;
import com.openshift.cloud.api.kas.auth.models.TopicsList;
import com.openshift.cloud.api.kas.models.KafkaRequest;
import com.openshift.cloud.api.kas.models.KafkaRequestList;
import com.openshift.cloud.api.serviceaccounts.models.ServiceAccountData;
import com.openshift.cloud.api.srs.models.Registry;
import com.openshift.cloud.api.srs.models.RegistryList;
import io.managed.services.test.Environment;
import io.managed.services.test.RetryUtils;
import io.managed.services.test.ThrowingSupplier;
import lombok.SneakyThrows;
import com.openshift.cloud.api.kas.auth.models.Record;
import lombok.extern.log4j.Log4j2;
import org.openapitools.jackson.nullable.JsonNullableModule;
import org.testng.Assert;


import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static java.time.Duration.ofMinutes;
import static lombok.Lombok.sneakyThrow;

@Log4j2
public class CLI {

    private static final Duration DEFAULT_TIMEOUT = ofMinutes(3);

    private static final String CLUSTER_CAPACITY_EXHAUSTED_CODE = "KAFKAS-MGMT-24";

    private final String workdir;
    private final String cmd;

    public CLI(Path binary) {
        this.workdir = binary.getParent().toString();
        this.cmd = String.format("./%s", binary.getFileName().toString());
    }

    public String getWorkdir() {
        return this.workdir;
    }

    private ProcessBuilder builder(List<String> command) {
        var cmd = new ArrayList<String>();
        cmd.add(this.cmd);
        cmd.add("-v");
        cmd.addAll(command);

        return new ProcessBuilder(cmd)
            .directory(new File(workdir));
    }

    private AsyncProcess exec(String... command) throws CliGenericException {
        return exec(List.of(command));
    }

    private AsyncProcess exec(List<String> command) throws CliGenericException {
        try {
            return execAsync(command).sync(DEFAULT_TIMEOUT);
        } catch (ProcessException e) {
            throw CliGenericException.exception(e);
        }
    }

    private AsyncProcess execAsync(String... command) {
        return execAsync(List.of(command));
    }

    private AsyncProcess execAsync(List<String> command) {
        try {
            return new AsyncProcess(builder(command).start());
        } catch (IOException e) {
            throw sneakyThrow(e);
        }
    }

    /**
     * This method only starts the CLI login, use CLIUtils.login() instead of this method
     * to login using username and password
     */
    public AsyncProcess login(String apiGateway, String authURL, boolean insecure, String token) {

        List<String> cmd = new ArrayList<>();
        cmd.add("login");

        if (apiGateway != null) {
            cmd.addAll(List.of("--api-gateway", apiGateway));
        }

        if (authURL != null) {
            cmd.addAll(List.of("--auth-url", authURL));
        }

        if (token != null) {
            cmd.addAll(List.of("--token", token));
        }

        if (insecure) {
            cmd.add("--insecure");
        }

        cmd.add("--print-sso-url");

        return execAsync(cmd);
    }

    @SneakyThrows
    public void logout() {
        exec("logout");
    }

    @SneakyThrows
    public String help() {
        return exec("--help").stdoutAsString();
    }

    public KafkaRequest createKafka(String name) throws CliGenericException {
        return retryKafkaCreation(() -> exec("kafka", "create", "--bypass-checks", "--name", name, "--provider", Environment.CLOUD_PROVIDER, "--region", Environment.DEFAULT_KAFKA_REGION))
            .asJson(KafkaRequest.class);
    }

    public void deleteKafka(String id) throws CliGenericException {
        retry(() -> exec("kafka", "delete", "--id", id, "-y"));
    }

    public KafkaRequest describeKafka(String id) throws CliGenericException {
        return retry(() -> exec("kafka", "describe", "--id", id))
            .asJson(KafkaRequest.class);
    }

    public void useKafka(String id) throws CliGenericException {
        retry(() -> exec("kafka", "use", "--id", id));
    }

    public KafkaRequestList listKafka() throws CliGenericException {
        return retry(() -> exec("kafka", "list", "-o", "json"))
            .asJson(KafkaRequestList.class);
    }

    public KafkaRequestList searchKafkaByName(String name) throws CliGenericException {
        var p = retry(() -> exec("kafka", "list", "--search", name, "-o", "json"));
        if (p.stderrAsString().contains("No Kafka instances were found")) {
            return new KafkaRequestList();
        }
        return p.asJson(KafkaRequestList.class);
    }

    public ServiceAccountData describeServiceAccount(String id) throws CliGenericException {
        return retry(() -> exec("service-account", "describe", "--id", id))
            .asJson(ServiceAccountData.class);
    }

    public ServiceAccountData[] listServiceAccount() throws CliGenericException {
        return retry(() -> exec("service-account", "list", "-o", "json"))
            .asJson(ServiceAccountData[].class);
    }

    public void deleteServiceAccount(String id) throws CliGenericException {
        retry(() -> exec("service-account", "delete", "--id", id, "-y"));
    }

    public void createServiceAccount(String name, Path path) throws CliGenericException {
        retry(() -> exec("service-account", "create", "--short-description", name, "--file-format", "json", "--output-file", path.toString(), "--overwrite"));
    }

    public Topic createTopic(String topicName) throws CliGenericException {
        return retry(() -> exec("kafka", "topic", "create", "--name", topicName, "-o", "json"))
            .asJson(Topic.class);
    }

    public Topic createTopic(String topicName, int partitions) throws CliGenericException {
        return retry(() -> exec("kafka", "topic", "create", "--name", topicName, "--partitions", String.valueOf(partitions), "-o", "json"))
                .asJson(Topic.class);
    }

    public void deleteTopic(String topicName) throws CliGenericException {
        retry(() -> exec("kafka", "topic", "delete", "--name", topicName, "-y"));
    }

    public TopicsList listTopics() throws CliGenericException {
        return retry(() -> exec("kafka", "topic", "list", "-o", "json"))
            .asJson(TopicsList.class);
    }

    public Topic describeTopic(String topicName) throws CliGenericException {
        return retry(() -> exec("kafka", "topic", "describe", "--name", topicName, "-o", "json"))
            .asJson(Topic.class);
    }

    public void updateTopic(String topicName, String retentionTime) throws CliGenericException {
        retry(() -> exec("kafka", "topic", "update", "--name", topicName, "--retention-ms", retentionTime));
    }

    public ConsumerGroupList listConsumerGroups() throws CliGenericException {
        return retry(() -> exec("kafka", "consumer-group", "list", "-o", "json"))
            .asJson(ConsumerGroupList.class);
    }

    public void deleteConsumerGroup(String id) throws CliGenericException {
        retry(() -> exec("kafka", "consumer-group", "delete", "--id", id, "-y"));
    }

    public ConsumerGroup describeConsumerGroup(String name) throws CliGenericException {
        return retry(() -> exec("kafka", "consumer-group", "describe", "--id", name, "-o", "json"))
            .asJson(ConsumerGroup.class);
    }

    public void connectCluster(String token, String kubeconfig, String serviceType) throws CliGenericException {
        retry(() -> exec("cluster", "connect", "--token", token, "--kubeconfig", kubeconfig, "--service-type", serviceType, "-y"));
    }

    public void grantProducerAndConsumerAccess(String userName, String topic, String group) throws CliGenericException {
        retry(() -> exec("kafka", "acl", "grant-access", "-y", "--producer", "--consumer", "--user", userName, "--topic", topic, "--group", group));
    }

    public AclBindingListPage listACLs() throws CliGenericException {
        return retry(() -> exec("kafka", "acl", "list", "-o", "json"))
            .asJson(AclBindingListPage.class);
    }

    public Registry createServiceRegistry(String name) throws CliGenericException {
        return retry(() -> exec("service-registry", "create", "--name", name))
                .asJson(Registry.class);
    }

    public Registry describeServiceRegistry(String id) throws CliGenericException  {
        return retry(() -> exec("service-registry", "describe", "--id", id))
                .asJson(Registry.class);
    }

    /**
     * Return the Registry in use from the CLI
     */
    public Registry describeServiceRegistry() throws CliGenericException {
        return retry(() -> exec("service-registry", "describe"))
                .asJson(Registry.class);
    }

    public RegistryList listServiceRegistry() throws CliGenericException {
        return retry(() -> exec("service-registry", "list", "-o", "json"))
                .asJson(RegistryList.class);
    }

    public void useServiceRegistry(String id) throws CliGenericException {
        retry(() -> exec("service-registry", "use", "--id", id));
    }

    public void deleteServiceRegistry(String name) throws CliGenericException {
        retry(() -> exec("service-registry", "delete", "--id", name, "-y"));
    }

    public List<Record> consumeRecords(String topicName, String instanceId, int partition, int offset) throws CliGenericException, JsonProcessingException {
        List<String> cmd = List.of("kafka", "topic", "consume",
                "--instance-id", instanceId,
                "--name", topicName,
                "--offset", Integer.toString(offset),
                "--partition", Integer.toString(partition),
                "--format", "json"
        );

        return consumeRecords(cmd);
    }

    public List<Record> consumeRecords(String topicName, String instanceId, int partition) throws CliGenericException, JsonProcessingException {
        List<String> cmd = List.of("kafka", "topic", "consume",
                "--instance-id", instanceId,
                "--name", topicName,
                "--partition", Integer.toString(partition),
                "--format", "json"
        );

        return consumeRecords(cmd);
    }

    public Record produceRecords(String topicName, String instanceId, String message, int partition, String recordKey)
            throws InterruptedException, ExecutionException, IOException {
        List<String> cmd = List.of("kafka", "topic", "produce",
                "--instance-id", instanceId,
                "--name", topicName,
                "--partition", Integer.toString(partition),
                "--key", recordKey
        );
        return produceRecords(message, cmd);
    }

    public Record produceRecords(String topicName, String instanceId, String message)
            throws IOException, ExecutionException, InterruptedException {
        List<String> cmd = List.of("kafka", "topic", "produce",
                "--instance-id", instanceId,
                "--name", topicName
        );
        return produceRecords(message, cmd);
    }

    private Record produceRecords(String message, List<String> commands) throws IOException, ExecutionException, InterruptedException {
        var produceMessageProcess = execAsync(commands);
        var stdin = produceMessageProcess.stdin();

        // write message
        stdin.write(message);
        stdin.close();

        // return code
        var returnedCode = produceMessageProcess.future(Duration.ofSeconds(10)).get().waitFor();

        // Read Record object
        Record producedRecord = produceMessageProcess.asJson(Record.class);

        // Assert correctness
        Assert.assertEquals(returnedCode, 0);

        return producedRecord;
    }

    private List<Record> consumeRecords(List<String> cmd) throws CliGenericException, JsonProcessingException {

        // consume returns inline Jsons separated by newline, therefore string must be firstly split than read as multiple jsons
        var output = retry(() -> exec(cmd)).stdoutAsString();

        // specific separated JSON objects \n}\n which is separator of multiple inline jsons
        String[] lines = output.split("\n\\}\n");
        // append back '}' (i.e. curly bracket) so JSON objects will not miss this end symbol
        List<String> messagesWithFixedFormat =  Arrays.stream(lines).map(in -> in + "}").collect(Collectors.toList());

        var objectMapper =  new ObjectMapper()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                .registerModule(new JavaTimeModule())
                .registerModule(new JsonNullableModule());
        List<Record> records = new ArrayList<>();

        // each object is read as separated Record
        for (String line: messagesWithFixedFormat) {
            Record record = objectMapper.readValue(line, Record.class);
            records.add(record);
        }
        return records;
    }

    private <T, E extends Throwable> T retry(ThrowingSupplier<T, E> call) throws E {
        return RetryUtils.retry(1, call, CLI::retryCondition);
    }

    private <T, E extends Throwable> T retryKafkaCreation(ThrowingSupplier<T, E> call) throws E {
        return RetryUtils.retry(
                1, null, call, CLI::retryConditionKafkaCreation, 12, Duration.ofSeconds(10));
    }

    private static boolean retryConditionKafkaCreation(Throwable t) {
        if (t instanceof CliGenericException) {
            // quota problem,
            if (((CliGenericException) t).getCode() == 403 && ((CliGenericException) t).getMessage().contains(CLUSTER_CAPACITY_EXHAUSTED_CODE)) {
                return true;
            }
            // server side problem
            return ((CliGenericException) t).getCode() >= 500 && ((CliGenericException) t).getCode() < 600;
        }
        return false;
    }

    private static boolean retryCondition(Throwable t) {
        if (t instanceof CliGenericException) {
            return ((CliGenericException) t).getCode() >= 500 && ((CliGenericException) t).getCode() < 600;
        }
        return false;
    }
}
