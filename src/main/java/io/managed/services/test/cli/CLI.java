package io.managed.services.test.cli;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.openshift.cloud.api.kas.auth.models.AclBindingListPage;
import com.openshift.cloud.api.kas.auth.models.AclOperation;
import com.openshift.cloud.api.kas.auth.models.AclPermissionType;
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
import java.util.Locale;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import static java.time.Duration.ofMinutes;
import static lombok.Lombok.sneakyThrow;


@Log4j2
public class CLI {

    private static final Duration DEFAULT_TIMEOUT = ofMinutes(3);

    private static final String CLUSTER_CAPACITY_EXHAUSTED_CODE = "KAFKAS-MGMT-24";
    
    private static final Locale LOCALE_EN = Locale.ENGLISH;

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
    public AsyncProcess login(String apiGateway, String authURL, boolean insecure) {

        List<String> cmd = new ArrayList<>();
        cmd.add("login");

        if (apiGateway != null) {
            cmd.addAll(List.of("--api-gateway", apiGateway));
        }

        if (authURL != null) {
            cmd.addAll(List.of("--auth-url", authURL));
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
            .parseNodeFromProcessOutput()
            .getObjectValue(KafkaRequest::createFromDiscriminatorValue);
    }

    public void deleteKafka(String id) throws CliGenericException {
        retry(() -> exec("kafka", "delete", "--id", id, "-y"));
    }

    public KafkaRequest describeKafkaById(String id) throws CliGenericException {
        return retry(() -> exec("kafka", "describe", "--id", id))
            .parseNodeFromProcessOutput()
            .getObjectValue(KafkaRequest::createFromDiscriminatorValue);
    }

    public KafkaRequest describeKafkaByName(String name) throws CliGenericException {
        return retry(() -> exec("kafka", "describe", "--name", name))
            .parseNodeFromProcessOutput()
            .getObjectValue(KafkaRequest::createFromDiscriminatorValue);
    }

    public void useKafka(String id) throws CliGenericException {
        retry(() -> exec("kafka", "use", "--id", id));
    }

    public KafkaRequestList listKafka() throws CliGenericException {
        return retry(() -> exec("kafka", "list", "-o", "json"))
            .parseNodeFromProcessOutput()
            .getObjectValue(KafkaRequestList::createFromDiscriminatorValue);
    }

    public KafkaRequestList searchKafkaByName(String name) throws CliGenericException {
        return retry(() -> exec("kafka", "list", "--search", name, "-o", "json"))
            .parseNodeFromProcessOutput()
            .getObjectValue(KafkaRequestList::createFromDiscriminatorValue);
    }

    public ServiceAccountData describeServiceAccount(String id) throws CliGenericException {
        return retry(() -> exec("service-account", "describe", "--id", id))
            .parseNodeFromProcessOutput()
            .getObjectValue(ServiceAccountData::createFromDiscriminatorValue);
    }

    public List<ServiceAccountData> listServiceAccount() throws CliGenericException {
        return retry(() -> exec("service-account", "list", "-o", "json"))
                .parseNodeFromProcessOutput()
                .getCollectionOfObjectValues(ServiceAccountData::createFromDiscriminatorValue);
    }

    public void deleteServiceAccount(String id) throws CliGenericException {
        retry(() -> exec("service-account", "delete", "--id", id, "-y"));
    }

    public void createServiceAccount(String name, Path path) throws CliGenericException {
        retry(() -> exec("service-account", "create", "--short-description", name, "--file-format", "json", "--output-file", path.toString(), "--overwrite"));
    }

    public Topic createTopic(String topicName) throws CliGenericException {
        return retry(() -> exec("kafka", "topic", "create", "--name", topicName, "-o", "json"))
            .parseNodeFromProcessOutput()
            .getObjectValue(Topic::createFromDiscriminatorValue);
    }

    public Topic createTopic(String topicName, int partitions) throws CliGenericException {
        return retry(() -> exec("kafka", "topic", "create", "--name", topicName, "--partitions", String.valueOf(partitions), "-o", "json"))
            .parseNodeFromProcessOutput()
            .getObjectValue(Topic::createFromDiscriminatorValue);
    }

    public void deleteTopic(String topicName) throws CliGenericException {
        retry(() -> exec("kafka", "topic", "delete", "--name", topicName, "-y"));
    }

    public TopicsList listTopics() throws CliGenericException {
        return retry(() -> exec("kafka", "topic", "list", "-o", "json"))
            .parseNodeFromProcessOutput()
            .getObjectValue(TopicsList::createFromDiscriminatorValue);
    }

    public Topic describeTopic(String topicName) throws CliGenericException {
        return retry(() -> exec("kafka", "topic", "describe", "--name", topicName, "-o", "json"))
            .parseNodeFromProcessOutput()
            .getObjectValue(Topic::createFromDiscriminatorValue);
    }

    public void updateTopic(String topicName, String retentionTime) throws CliGenericException {
        retry(() -> exec("kafka", "topic", "update", "--name", topicName, "--retention-ms", retentionTime));
    }

    public ConsumerGroupList listConsumerGroups() throws CliGenericException {
        return retry(() -> exec("kafka", "consumer-group", "list", "-o", "json"))
            .parseNodeFromProcessOutput()
            .getObjectValue(ConsumerGroupList::createFromDiscriminatorValue);
    }

    public void deleteConsumerGroup(String id) throws CliGenericException {
        retry(() -> exec("kafka", "consumer-group", "delete", "--id", id, "-y"));
    }

    public ConsumerGroup describeConsumerGroup(String name) throws CliGenericException {
        return retry(() -> exec("kafka", "consumer-group", "describe", "--id", name, "-o", "json"))
            .parseNodeFromProcessOutput()
            .getObjectValue(ConsumerGroup::createFromDiscriminatorValue);
    }

    public void connectCluster(String token, String kubeconfig, String serviceType) throws CliGenericException {
        retry(() -> exec("cluster", "connect", "--token", token, "--kubeconfig", kubeconfig, "--service-type", serviceType, "-y"));
    }

    // kafka acl
    public enum ACLEntityType {
        USER("user", "--user"),
        SERVICE_ACCOUNT("service account", "--service-account");

        public final String name;
        public final String flag;

        private ACLEntityType(String name, String flag) {
            this.name = name;
            this.flag = flag;
        }
    }
    //// kafka acl create
    public void createAcl(ACLEntityType aclEntityType, String entityIdentificator, AclOperation operation, AclPermissionType permission, String topic) throws CliGenericException {
        retry(() -> exec("kafka", "acl", "create", "-y", aclEntityType.flag, entityIdentificator, "--topic", topic, "--permission", permission.toString().toLowerCase(LOCALE_EN), "--operation", operation.toString().toLowerCase(LOCALE_EN)));
    }

    //// kafka acl list
    public AclBindingListPage listACLs() throws CliGenericException {
        return retry(() -> exec("kafka", "acl", "list", "-o", "json"))
            .parseNodeFromProcessOutput()
            .getObjectValue(AclBindingListPage::createFromDiscriminatorValue);
    }

    public AclBindingListPage listACLs(ACLEntityType aclEntityType, String entityIdentificator) throws CliGenericException {
        return retry(() -> exec("kafka", "acl", "list", aclEntityType.flag, entityIdentificator, "-o", "json"))
            .parseNodeFromProcessOutput()
            .getObjectValue(AclBindingListPage::createFromDiscriminatorValue);
    }

    //// kafka acl delete
    public void deleteAcl(ACLEntityType aclEntityType, String entityIdentificator, AclOperation operation, AclPermissionType permission) throws CliGenericException {
        retry(() -> exec("kafka", "acl", "delete", "-y", aclEntityType.flag, entityIdentificator, "--permission", permission.toString().toLowerCase(LOCALE_EN), "--operation", operation.toString().toLowerCase(LOCALE_EN)));
    }

    public void deleteAllAcls(ACLEntityType aclEntityType, String entityIdentificator) throws CliGenericException {
        retry(() -> exec("kafka", "acl", "delete", "-y", aclEntityType.flag, entityIdentificator));
    }

    //// kafka acl grant-access
    public void grantAccessAcl(ACLEntityType aclEntityType, String entityIdentificator, String topic, String group) throws CliGenericException {
        retry(() -> exec("kafka", "acl", "grant-access", "-y", "--producer", "--consumer", aclEntityType.flag, entityIdentificator, "--topic", topic, "--group", group));
    }

    //// kafka acl grant-admin
    public void grantAdminAcl(ACLEntityType aclEntityType, String entityIdentificator) throws CliGenericException {
        retry(() -> exec("kafka", "acl", "grant-admin", "-y", aclEntityType.flag, entityIdentificator));
    }

    /**
     * Return the Registry in use from the CLI
     */
    public Registry createServiceRegistry(String name) throws CliGenericException {
        return retry(() -> exec("service-registry", "create", "--name", name))
            .parseNodeFromProcessOutput()
            .getObjectValue(Registry::createFromDiscriminatorValue);
    }

    public Registry describeServiceRegistry(String id) throws CliGenericException  {
        return retry(() -> exec("service-registry", "describe", "--id", id))
            .parseNodeFromProcessOutput()
            .getObjectValue(Registry::createFromDiscriminatorValue);
    }

    public Registry describeServiceRegistry() throws CliGenericException {
        return retry(() -> exec("service-registry", "describe"))
            .parseNodeFromProcessOutput()
            .getObjectValue(Registry::createFromDiscriminatorValue);
    }

    public RegistryList listServiceRegistry() throws CliGenericException {
        return retry(() -> exec("service-registry", "list", "-o", "json"))
            .parseNodeFromProcessOutput()
            .getObjectValue(RegistryList::createFromDiscriminatorValue);
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

        if (output.isEmpty()) {
            return new ArrayList<Record>();
        }
        
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
