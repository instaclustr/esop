package com.instaclustr.esop.impl;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.Adler32;

import com.google.common.collect.ImmutableList;

import org.apache.commons.lang3.tuple.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.instaclustr.esop.impl.hash.HashService;
import com.instaclustr.esop.impl.hash.HashServiceImpl;
import com.instaclustr.esop.impl.hash.HashSpec;
import com.instaclustr.io.FileUtils;

import static java.lang.Math.toIntExact;

public class SSTableUtils {

    private static final Logger logger = LoggerFactory.getLogger(SSTableUtils.class);

    // Ver. 2.0 = instaclustr-recovery_codes-jb-1-Data.db
    // Ver. 2.1 = lb-1-big-Data.db
    // Ver. 2.2 = lb-1-big-Data.db
    // Ver. 3.0 = mc-1-big-Data.db
    public static final Pattern SSTABLE_RE = Pattern.compile("((?:[a-zA-Z0-9][a-zA-Z0-9_-]+[a-zA-Z0-9][a-zA-Z0-9_-]+-)?[a-z]{2}-(\\d+)(?:-big)?)-.*");
    private static final ImmutableList<String> DIGESTS = ImmutableList.of("crc32", "adler32", "sha1");
    private static final int SSTABLE_PREFIX_IDX = 1;
    private static final int SSTABLE_GENERATION_IDX = 2;
    private static final Pattern CHECKSUM_RE = Pattern.compile("^([a-zA-Z0-9]+).*");
    private static final HashService hashService = new HashServiceImpl(new HashSpec());

    public static String sstableHash(Path path) throws IOException {
        final Matcher matcher = SSTABLE_RE.matcher(path.getFileName().toString());
        if (!matcher.matches()) {
            throw new IllegalStateException("Can't compute SSTable hash for " + path + ": doesn't taste like sstable");
        }

        for (String digest : DIGESTS) {
            final Path digestPath = path.resolveSibling(matcher.group(SSTABLE_PREFIX_IDX) + "-Digest." + digest);
            if (!Files.exists(digestPath)) {
                continue;
            }

            final Matcher matcherChecksum = CHECKSUM_RE.matcher(new String(Files.readAllBytes(digestPath), StandardCharsets.UTF_8));
            if (matcherChecksum.matches()) {
                return matcher.group(SSTABLE_GENERATION_IDX) + "-" + matcherChecksum.group(1);
            }
        }

        // Ver. 2.0 doesn't create hash file, so do it ourselves
        try {
            final Path dataFilePath = path.resolveSibling(matcher.group(SSTABLE_PREFIX_IDX) + "-Data.db");
            logger.warn("No digest file found, generating checksum based on {}.", dataFilePath);
            return matcher.group(SSTABLE_GENERATION_IDX) + "-" + calculateChecksum(dataFilePath);
        } catch (IOException e) {
            throw new IllegalStateException("Couldn't generate checksum for " + path);
        }
    }

    public static String calculateChecksum(final Path filePath) throws IOException {
        try (final FileChannel fileChannel = FileChannel.open(filePath)) {

            int bytesStart;
            int bytesPerChecksum = 10 * 1024 * 1024;

            // Get last 10 megabytes of file to use for checksum
            if (fileChannel.size() >= bytesPerChecksum) {
                bytesStart = toIntExact(fileChannel.size()) - bytesPerChecksum;
            } else {
                bytesStart = 0;
                bytesPerChecksum = (int) fileChannel.size();
            }

            fileChannel.position(bytesStart);
            final ByteBuffer bytesToChecksum = ByteBuffer.allocate(bytesPerChecksum);
            int bytesRead = fileChannel.read(bytesToChecksum, bytesStart);

            assert (bytesRead == bytesPerChecksum);

            // Adler32 because it's faster than SHA / MD5 and Cassandra uses it - https://issues.apache.org/jira/browse/CASSANDRA-5862
            final Adler32 adler32 = new Adler32();
            adler32.update(bytesToChecksum.array());

            return String.valueOf(adler32.getValue());
        }
    }

    public static Map<String, List<ManifestEntry>> getSSTables(String keyspace,
                                                               String table,
                                                               Path snapshotDirectory,
                                                               Path tableBackupPath,
                                                               HashSpec hashSpec) throws IOException {
        if (!Files.exists(snapshotDirectory)) {
            return Collections.emptyMap();
        }

        final HashService hashService = new HashServiceImpl(hashSpec);

        return Files.list(snapshotDirectory)
                    .flatMap(path -> {
                        if (isCassandra22SecIndex(path)) {
                            return FileUtils.tryListFiles(path);
                        }
                        return Stream.of(path);
                    })
                    .filter(path -> SSTABLE_RE.matcher(path.getFileName().toString()).matches())
                    .sorted()
                    .collect(Collectors.groupingBy(path -> {
                        final Matcher matcher = SSTABLE_RE.matcher(path.getFileName().toString());
                        if (matcher.matches()) {
                            return matcher.group(1);
                        } else {
                            return "";
                        }
                    }))
                    .entrySet()
                    .stream()
                    .filter(entry -> !entry.getKey().equals(""))
                    .map(entry -> {
                        try {
                            final String sstableBaseName = entry.getKey();
                            final List<ManifestEntry> entries = new ArrayList<>();

                            for (final Path sstableComponent : entry.getValue()) {
                                final String hash = sstableHash(sstableComponent);
                                final Path manifestComponentFileName = snapshotDirectory.relativize(sstableComponent);

                                final Path parent = manifestComponentFileName.getParent();

                                Path backupPath = tableBackupPath;

                                if (parent != null) {
                                    backupPath = backupPath.resolve(parent);
                                }

                                backupPath = backupPath.resolve(hash).resolve(manifestComponentFileName.getFileName());
                                final String hashOfFile = hashService.hash(sstableComponent);

                                entries.add(new ManifestEntry(backupPath,
                                                              sstableComponent,
                                                              ManifestEntry.Type.FILE,
                                                              hashOfFile,
                                                              new KeyspaceTable(keyspace, table),
                                                              null));
                            }

                            return Pair.of(sstableBaseName, entries);
                        } catch (Exception e) {
                            throw new UncheckedIOException(new IOException(e));
                        }
                    }).collect(Collectors.toMap(Pair::getKey, Pair::getValue));
    }

    /**
     * Checks whether or not the given table path leads to a secondary index folder (for Cassandra 2.2 +)
     */
    private static boolean isCassandra22SecIndex(final Path tablepath) {
        return tablepath.toFile().isDirectory() && tablepath.getFileName().toString().startsWith(".");
    }

    /**
     * Decides whether or not the manifest path includes secondary index files
     *
     * @param manifestPath path to manifest
     * @return true if manifest path includes secondary index files, false otherwise
     */
    public static boolean isSecondaryIndexManifest(final Path manifestPath) {
        // When there's a secondary index, manifest path contains 6 elements (including '.indexName' and 'hashcode')
        // '.indexName' is filtered by subpath(3,4), to avoid the other parts of the manifest path getting misidentified with the '.'
        return manifestPath.getNameCount() == 6 && manifestPath.subpath(3, 4).toString().startsWith(".");
    }

    public static boolean isExistingSStable(final Path localPath, final String sstable) {
        try {
            if (localPath.toFile().exists() && sstableHash(localPath).equals(sstable)) {
                return true;
            }
        } catch (IOException e) {
            // SSTableUtils.sstableHash may throw exception if SSTable has not been probably downloaded
            logger.error(e.getMessage());
        }
        return false;
    }
}
