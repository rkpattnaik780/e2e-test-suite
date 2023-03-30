package io.managed.services.test.cli;

import io.managed.services.test.Environment;
import io.managed.services.test.client.github.Asset;
import io.managed.services.test.client.github.GitHub;
import io.managed.services.test.client.github.Release;
import lombok.SneakyThrows;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;

import static io.managed.services.test.cli.CLIUtils.extractCLI;

public class CLIDownloader {
    private static final Logger LOGGER = LogManager.getLogger(CLIDownloader.class);

    private static final String TMPDIR = "cli";
    private static final String NAME = "rhoas";

    private static final String DOWNLOAD_ASSET_TEMPLATE = "^%s_\\S+_%s_%s.%s$"; // rhoas_{version}_linux_amd64.tar.gz
    private static final String ARCHIVE_ENTRY_TEMPLATE = "^%s_\\S+_%s_%s/%s$"; // rhoas_{version}_linux_amd64/rhoas

    private final GitHub github;
    private final String organization;
    private final String repository;
    private final String version;
    private final String platform;
    private final String arch;
    private final String archiveExt;
    private final String excludeVersions;

    public CLIDownloader(
        String organization,
        String repository,
        String version,
        String platform,
        String arch,
        String excludeVersions) {

        this.github = new GitHub();
        this.organization = organization;
        this.repository = repository;
        this.version = version;
        this.platform = platform;
        this.arch = arch;
        this.archiveExt = platformToArchiveExt(platform);
        this.excludeVersions = excludeVersions;
    }

    public static CLIDownloader defaultDownloader() {
        return new CLIDownloader(
            Environment.CLI_DOWNLOAD_ORG,
            Environment.CLI_DOWNLOAD_REPO,
            Environment.CLI_VERSION,
            Environment.CLI_PLATFORM,
            Environment.CLI_ARCH,
            Environment.CLI_EXCLUDE_VERSIONS);
    }

    private String platformToArchiveExt(String platform) {
        if (Platform.WINDOWS.toString().equals(platform)) {
            return "zip";
        }

        return "tar.gz";
    }

    @SneakyThrows
    public Path downloadCLIInTempDir() {
        LOGGER.info("download CLI in temporary directory");
        var workspace = Files.createTempDirectory(TMPDIR);

        LOGGER.info("download CLI in workspace: {}", workspace);
        var release = github.getReleaseByTagName(organization, repository, version, excludeVersions);

        var asset = getDownloadAssetFromRelease(release);

        LOGGER.info("download asset '{}'", asset.toString());
        var archive = github.downloadAsset(organization, repository, asset.getId());

        var binary = extractCLIBinaryInWorkspace(archive, workspace);

        makeCLIBinaryExecutable(binary);

        return binary;
    }

    private Path extractCLIBinaryInWorkspace(InputStream archive, Path workspace) throws IOException {
        var entry = String.format(ARCHIVE_ENTRY_TEMPLATE, NAME, platform, arch, NAME);
        var binary = workspace.resolve(NAME);

        LOGGER.info("extract {} from stream archive to: {}", entry, binary);
        extractCLI(archive, entry, binary);
        return binary;
    }

    private void makeCLIBinaryExecutable(Path binary) throws IOException {
        if (Platform.WINDOWS.toString().equals(platform)) {
            // skip windows
            return;
        }

        Files.setPosixFilePermissions(binary, PosixFilePermissions.fromString("rwxr-xr-x"));
    }

    private Asset getDownloadAssetFromRelease(Release release) {
        var assetName = String.format(DOWNLOAD_ASSET_TEMPLATE, NAME, platform, arch, archiveExt);
        LOGGER.info("search for asset '{}' in release: '{}'", assetName, release.toString());
        return release.getAssets().stream()
            .filter(a -> a.getName().matches(assetName))
            .findFirst()
            .orElseThrow(() -> new RuntimeException(String.format("asset '%s' not found in release: %s", assetName, release)));
    }
}
