package io.managed.services.test.client.github;

import io.managed.services.test.client.exception.ClientException;
import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.ClientBuilder;
import jakarta.ws.rs.client.WebTarget;
import jakarta.ws.rs.core.Response;
import org.apache.http.HttpHeaders;
import java.io.InputStream;
import java.util.List;
import java.util.Locale;
import java.util.NoSuchElementException;
import java.util.regex.Pattern;

public class GitHub implements AutoCloseable {

    private final static String GITHUB_URL = "https://api.github.com";

    private final Client client;
    private final WebTarget github;

    public GitHub(String token) {
        this.client = ClientBuilder.newClient();

        var t = client.target(GITHUB_URL);
        if (token != null) {
            t = t.register(new BearerAuthFilter(token));
        }
        this.github = t;
    }

    private List<Release> getReleases(String org, String repo) {
        var path = String.format("/repos/%s/%s/releases", org, repo);

        var r = github.path(path).request().get(Release[].class);
        return List.of(r);
    }

    public Release getLatestRelease(String org, String repo, Pattern exclude) {
        var releases = this.getReleases(org, repo);

        // get the first release that is not a draft
        return releases.stream()
            .filter(r -> !r.getDraft())

            // ignore releases without assets because the release pipeline hasn't yet finished building them
            .filter(r -> !r.getAssets().isEmpty())

            // exclude releases matching the exclude regex
            .filter(r -> !exclude.matcher(r.getName()).find())

            .findFirst()
            .orElseThrow(() -> new NoSuchElementException(String.format("latest release not found in repository: %s/%s", org, repo)));
    }

    public Release getReleaseByTagName(String org, String repo, String name, String exclude) {
        if (name.toLowerCase(Locale.ROOT).equals("latest")) {
            return getLatestRelease(org, repo, Pattern.compile(exclude));
        }

        var path = String.format("/repos/%s/%s/releases/tags/%s", org, repo, name);
        return github.path(path).request().get(Release.class);
    }

    public InputStream downloadAsset(String org, String repo, String id) {
        var path = String.format("/repos/%s/%s/releases/assets/%s", org, repo, id);
        var r = github.path(path).request("application/octet-stream").get();

        if (!r.getStatusInfo().getFamily().equals(Response.Status.Family.REDIRECTION)) {
            throw new ClientException("expected redirect response", r);
        }

        var l = r.getHeaderString(HttpHeaders.LOCATION);
        if (l == null) {
            throw new ClientException("Location header not found", r);
        }
        return client.target(l).request().get(InputStream.class);
    }

    @Override
    public void close() {
        client.close();
    }
}
