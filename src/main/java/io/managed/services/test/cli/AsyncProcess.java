package io.managed.services.test.cli;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.microsoft.kiota.serialization.JsonParseNodeFactory;
import com.microsoft.kiota.serialization.ParseNode;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.io.input.TeeInputStream;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.openapitools.jackson.nullable.JsonNullableModule;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static lombok.Lombok.sneakyThrow;

@Log4j2
public class AsyncProcess {

    private final Process process;
    private final ProcessHandle.Info info;

    /**
     * Joint output from stdout and stderr similar to what will be printed on a terminal
     */
    private final ByteArrayOutputStream joint = new ByteArrayOutputStream();

    private final ByteArrayOutputStream stdout = new ByteArrayOutputStream();
    private final ByteArrayOutputStream stderr = new ByteArrayOutputStream();

    private final InputStream liveStdout;
    private final InputStream liveStderr;
    private final OutputStream liveStdin;

    private final Exception cause;

    private CompletableFuture<Process> future;

    private boolean destroyed;

    public AsyncProcess(Process process) {
        this.info = process.info();
        this.process = process;

        this.liveStdout = new TeeInputStream(new TeeInputStream(process.getInputStream(), joint), stdout);
        this.liveStderr = new TeeInputStream(new TeeInputStream(process.getErrorStream(), joint), stderr);

        this.liveStdin = process.getOutputStream();

        this.cause = new Exception();
    }

    public CompletableFuture<Process> future(Duration timeout) {
        if (future != null) {
            return future;
        }

        future = process.onExit()

            // set a timeout for the process
            .orTimeout(timeout.toMillis(), TimeUnit.MILLISECONDS)

            // handle the process result
            .handle((p, t) -> {

                if (!destroyed) {
                    // if the process has been destroyed the stderr and stdout are closed
                    readAll();
                }

                if (process.isAlive()) {
                    process.destroyForcibly();
                }

                if (t != null) {
                    throw sneakyThrow(t);
                }

                if (p.exitValue() != 0) {
                    throw sneakyThrow(processException(p.exitValue(), cause));
                }
                return p;
            });
        return future;
    }

    public AsyncProcess sync(Duration timeout) throws ProcessException {

        try {
            future(timeout).get();
            return this;

        } catch (ExecutionException e) {
            var cause = e.getCause();
            if (cause instanceof ProcessException) {
                throw (ProcessException) cause;
            }
            throw sneakyThrow(cause);
        } catch (InterruptedException e) {
            throw sneakyThrow(e);
        }
    }

    public ProcessException processException(Exception cause) {
        return processException(process.exitValue(), cause);
    }

    private ProcessException processException(int exitCode, Exception cause) {
        return new ProcessException(
            info.commandLine().orElse(""),
            exitCode,
            stdoutAsString(),
            stderrAsString(),
            outputAsString(),
            cause);
    }

    public boolean isDead() {
        return !process.isAlive();
    }

    public BufferedWriter stdin() {
        return new BufferedWriter(new OutputStreamWriter(liveStdin));
    }

    public BufferedReader stdout() {
        return new BufferedReader(new InputStreamReader(liveStdout));
    }

    /**
     * @return the full output written to stdout till now
     */
    public String stdoutAsString() {
        return stdout.toString(StandardCharsets.UTF_8);
    }

    /**
     * @return the full output written to stderr till now
     */
    public String stderrAsString() {
        return stderr.toString(StandardCharsets.UTF_8);
    }

    public String outputAsString() {
        return joint.toString(StandardCharsets.UTF_8);
    }

    public <T> T asJson(Class<T> c) {
        try {
            return new ObjectMapper()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                .registerModule(new JavaTimeModule())
                .registerModule(new JsonNullableModule())
                .readValue(stdoutAsString(), c);
        } catch (JsonProcessingException e) {
            throw sneakyThrow(e);
        }
    }

    public ParseNode parseNodeFromProcessOutput() {
        InputStream targetStream = new java.io.ByteArrayInputStream(this.stdoutAsString().getBytes());
        log.debug("process stdout: ");
        log.debug(this.stdoutAsString());

        var factory = new JsonParseNodeFactory();
        return factory.getParseNode("application/json", targetStream);
    }

    public void readAll() {
        readAll(liveStdout);
        readAll(liveStderr);
    }

    private static void readAll(InputStream stream) {
        try {
            stream.readNBytes(stream.available());
        } catch (IOException e) {
            log.error("failed to read all remaining bytes form the input stream:", e);
        }
    }

    public void destroy() {

        if (isDead()) {
            return;
        }

        // save stdin and stdout before destroy the process
        readAll();

        this.destroyed = true;

        // destroy the process
        process.destroy();
    }
}
