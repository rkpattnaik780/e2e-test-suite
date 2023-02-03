package io.managed.services.test.cli;

import com.openshift.cloud.api.kas.auth.models.AclBinding;
import com.openshift.cloud.api.kas.auth.models.AclBindingListPage;
import com.openshift.cloud.api.kas.auth.models.AclOperation;
import com.openshift.cloud.api.kas.auth.models.AclPermissionType;
import com.openshift.cloud.api.kas.auth.models.AclResourceType;
import com.openshift.cloud.api.kas.auth.models.ConsumerGroup;
import com.openshift.cloud.api.kas.auth.models.Topic;
import com.openshift.cloud.api.kas.models.KafkaRequest;
import com.openshift.cloud.api.serviceaccounts.models.ServiceAccountData;
import com.openshift.cloud.api.srs.models.Registry;
import io.fabric8.kubernetes.api.model.AuthInfo;
import io.fabric8.kubernetes.api.model.Cluster;
import io.fabric8.kubernetes.api.model.Config;
import io.fabric8.kubernetes.api.model.Context;
import io.fabric8.kubernetes.api.model.NamedAuthInfo;
import io.fabric8.kubernetes.api.model.NamedCluster;
import io.fabric8.kubernetes.api.model.NamedContext;
import io.managed.services.test.Environment;
import io.managed.services.test.TestUtils;
import io.managed.services.test.client.kafkainstance.KafkaInstanceApiUtils;
import io.managed.services.test.client.kafkamgmt.KafkaMgmtApiUtils;
import io.managed.services.test.client.kafkamgmt.KafkaNotDeletedException;
import io.managed.services.test.client.kafkamgmt.KafkaNotReadyException;
import io.managed.services.test.client.kafkamgmt.KafkaUnknownHostsException;
import io.managed.services.test.client.oauth.KeycloakLoginSession;
import io.managed.services.test.client.registrymgmt.RegistryMgmtApiUtils;
import io.managed.services.test.client.registrymgmt.RegistryNotDeletedException;
import io.managed.services.test.client.registrymgmt.RegistryNotReadyException;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import lombok.SneakyThrows;

import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public class CLIUtils {
    private static final Logger LOGGER = LogManager.getLogger(CLIUtils.class);

    public static void extractCLI(InputStream archive, String entryMatch, Path dest) throws IOException {

        var bi = new BufferedInputStream(archive);
        var gzi = new GzipCompressorInputStream(bi);
        var tar = new TarArchiveInputStream(gzi);

        ArchiveEntry e;
        while ((e = tar.getNextEntry()) != null) {
            if (!e.getName().matches(entryMatch)) {
                continue;
            }

            LOGGER.info("extract {} to {}", e.getName(), dest);

            if (!tar.canReadEntryData(e)) {
                throw new IOException("can not read entry " + e.getName());
            }

            if (e.isDirectory()) {
                throw new IOException("the entry " + e.getName() + " is a directory");
            } else {
                var parent = dest.getParent().toFile();
                if (!parent.isDirectory() && !parent.mkdirs()) {
                    throw new IOException("failed to create directory " + parent);
                }
                Files.copy(tar, dest);
                return;
            }
        }
        throw new IOException("cli not found");
    }

    public static CompletableFuture<Void> login(Vertx vertx, CLI cli, String username, String password) {
        var session = new KeycloakLoginSession(vertx, username, password);
        return login(vertx, cli, session);
    }

    public static CompletableFuture<Void> login(Vertx vertx, CLI cli, KeycloakLoginSession session) {
        boolean insecure = Environment.KAFKA_INSECURE_TLS;
        return login(vertx, cli, session, insecure);
    }

    public static CompletableFuture<Void> login(Vertx vertx, CLI cli, KeycloakLoginSession session, boolean insecureLogin) {

        var authURL = String.format("%s/auth/realms/%s", Environment.REDHAT_SSO_URI, Environment.REDHAT_SSO_REALM);

        LOGGER.info("start CLI login with username: {}", session.getUsername());
        var process = cli.login(Environment.OPENSHIFT_API_URI, authURL, insecureLogin);

        LOGGER.info("start oauth login against CLI");
        var oauthFuture = parseUrl(vertx, process.stdout(), String.format("%s/auth/.*", Environment.REDHAT_SSO_URI))
            .compose(l -> session.login(l))
            .onSuccess(__ -> LOGGER.info("first oauth login completed"))
            .toCompletionStage().toCompletableFuture();

        var cliFuture = process.future(Duration.ofMinutes(3))
                .thenAccept(r -> LOGGER.info("CLI login completed"));

        return CompletableFuture.allOf(oauthFuture, cliFuture);
    }

    private static Future<String> parseUrl(Vertx vertx, BufferedReader stdout, String urlRegex) {

        return vertx.executeBlocking(h -> {
            var full = new ArrayList<String>();
            while (true) {
                try {
                    var l = stdout.readLine();
                    if (l == null) {
                        h.fail(new Exception("SSO URL Not found\n-- STDOUT --\n" + String.join("\n", full)));
                        return;
                    }

                    if (l.matches(urlRegex)) {
                        h.complete(l);
                        return;
                    }
                    full.add(l);

                } catch (Exception e) {
                    h.fail(e);
                    return;
                }
            }
        });
    }

    public static Optional<ConsumerGroup> getConsumerGroupByName(CLI cli, String consumerName) throws CliGenericException {
        try {
            return Optional.of(cli.describeConsumerGroup(consumerName));
        } catch (CliNotFoundException e) {
            return Optional.empty();
        }
    }

    public static KafkaRequest waitUntilKafkaIsReady(CLI cli, String id)
        throws KafkaUnknownHostsException, KafkaNotReadyException, InterruptedException, CliGenericException {

        return KafkaMgmtApiUtils.waitUntilKafkaIsReady(() -> cli.describeKafkaById(id));
    }

    public static void waitUntilKafkaIsDeleted(CLI cli, String id)
        throws KafkaNotDeletedException, InterruptedException, CliGenericException {

        KafkaMgmtApiUtils.waitUntilKafkaIsDeleted(() -> {
            try {
                return Optional.of(cli.describeKafkaById(id));
            } catch (CliNotFoundException e) {
                return Optional.empty();
            }
        });
    }

    public static void waitUntilRegistryIsDeleted(CLI cli, String registryId)
        throws InterruptedException, RegistryNotDeletedException, CliGenericException {

        RegistryMgmtApiUtils.waitUntilRegistryIsDeleted(() -> {
            try {
                return Optional.of(cli.describeServiceRegistry(registryId));
            } catch (CliNotFoundException e) {
                return Optional.empty();
            }
        });
    }

    public static Optional<ServiceAccountData> getServiceAccountByName(CLI cli, String name) throws CliGenericException {
        return Arrays.stream(cli.listServiceAccount()).filter(sa -> name.equals(sa.getName())).findAny();
    }

    public static ServiceAccountSecret createServiceAccount(CLI cli, String name) throws CliGenericException {
        var secretPath = Paths.get(cli.getWorkdir(), name + ".json");
        cli.createServiceAccount(name, secretPath);
        return getServiceAccountSecret(secretPath);
    }

    @SneakyThrows
    public static ServiceAccountSecret getServiceAccountSecret(Path secretPath) {
        return TestUtils.asJson(ServiceAccountSecret.class, Files.readString(secretPath));
    }

    @SneakyThrows
    public static ConsumerGroup waitForConsumerGroup(CLI cli, String name) {
        return KafkaInstanceApiUtils.waitForConsumerGroup(() -> getConsumerGroupByName(cli, name));
    }

    public static Registry waitUntilServiceRegistryIsReady(CLI cli, String id)
            throws InterruptedException, CliGenericException, RegistryNotReadyException {

        return RegistryMgmtApiUtils.waitUntilRegistryIsReady(() -> {
            try {
                return cli.describeServiceRegistry(id);
            } catch (CliGenericException e) {
                throw new RuntimeException(e);
            }
        });
    }

    public static Topic applyTopic(CLI cli, String topicName, int partitions) throws CliGenericException {

        // get topic, if exist
        var list = cli.listTopics();
        LOGGER.debug(list);
        var exists = Objects.requireNonNull(list.getItems()).stream()
                .filter(t -> topicName.equals(t.getName()))
                .findAny();

        //  if topic does not exist create new one
        if (exists.isEmpty()) {
            return cli.createTopic(topicName, partitions);
        }

        return exists.get();
    }

    public static Config kubeConfig(String server, String token, String namespace) {
        var cluster = new Cluster();
        cluster.setServer(server);
        cluster.setInsecureSkipTlsVerify(true);
        var namedCluster = new NamedCluster();
        namedCluster.setCluster(cluster);
        namedCluster.setName("default");

        var authInfo = new AuthInfo();
        authInfo.setToken(token);

        var namedAuthInfo = new NamedAuthInfo();
        namedAuthInfo.setUser(authInfo);
        namedAuthInfo.setName("default");

        var context = new Context();
        context.setCluster("default");
        context.setUser("default");
        context.setNamespace(namespace);

        var namedContext = new NamedContext();
        namedContext.setContext(context);
        namedContext.setName("default");

        var c = new Config();
        c.setApiVersion("v1");
        c.setKind("Config");
        c.setClusters(List.of(namedCluster));
        c.setUsers(List.of(namedAuthInfo));
        c.setContexts(List.of(namedContext));
        c.setCurrentContext("default");

        return c;
    }

    //// kafka acl search
    public static Optional<AclBinding> searchAcl(AclBindingListPage aclList, String principal, AclOperation operation, AclPermissionType permission, AclResourceType resourceType, String resourceName) {
        String userPrincipal = "User:".concat(principal);
        return Objects.requireNonNull(aclList.getItems())
            .stream()
            .filter(acl -> userPrincipal.equals(acl.getPrincipal()))
            .filter(acl -> permission.equals(acl.getPermission()))
            .filter(acl -> operation.equals(acl.getOperation()))
            .filter(acl -> resourceType.equals(acl.getResourceType()))
            .filter(acl -> resourceName.equals(acl.getResourceName()))
        .findAny();
    }
}
