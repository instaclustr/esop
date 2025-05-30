package com.instaclustr.esop.impl;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.text.CharacterIterator;
import java.text.StringCharacterIterator;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;
import java.util.stream.Stream;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import com.instaclustr.esop.impl.list.ListOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.instaclustr.esop.impl.Manifest.ManifestReporter.ManifestReport;
import com.instaclustr.esop.impl.ManifestEntry.Type;
import com.instaclustr.esop.impl.Snapshots.Snapshot;
import com.instaclustr.esop.impl.Snapshots.Snapshot.Keyspace;
import com.instaclustr.esop.impl.Snapshots.Snapshot.Keyspace.Table;
import com.instaclustr.esop.impl.backup.BaseBackupOperationRequest;

import static com.instaclustr.esop.impl.ManifestEntry.Type.MANIFEST_FILE;
import static java.lang.String.format;
import static java.util.stream.Collectors.toList;

public class Manifest implements Cloneable {

    private static final Logger logger = LoggerFactory.getLogger(Manifest.class);

    private Snapshot snapshot;

    @JsonIgnore
    private ManifestEntry manifest;

    private List<String> tokens;

    private String schemaVersion;

    private long size;

    public static Manifest from(final Snapshot snapshot) {
        return new Manifest(snapshot);
    }

    public Manifest() {
    }

    public Manifest(final Snapshot snapshot) {
        this.snapshot = snapshot;
    }

    public Snapshot getSnapshot() {
        return snapshot;
    }

    public void setSnapshot(final Snapshot snapshot) {
        this.snapshot = snapshot;
    }

    public ManifestEntry getManifest() {
        return manifest;
    }

    public void setManifest(final ManifestEntry manifest) {
        this.manifest = manifest;
    }

    public void setSchemaVersion(final String schemaVersion) {
        this.schemaVersion = schemaVersion;
    }

    public String getSchemaVersion() {
        return schemaVersion;
    }

    public void setTokens(final List<String> tokens) {
        this.tokens = tokens;
    }

    public List<String> getTokens() {
        return tokens;
    }

    public void setSize(long size) {
        this.size = size;
    }

    public long getSize() {
        return this.size;
    }

    @JsonIgnore
    public String getInitialTokensCassandraYamlFragment() {
        return "initial_token: " + String.join(",", getTokens());
    }

    public boolean hasSameTokens(final List<String> tokens) {
        if (tokens == null || this.tokens == null) {
            return false;
        }
        return this.tokens.size() == tokens.size() && this.tokens.containsAll(tokens);
    }

    @JsonIgnore
    public DatabaseEntities getDatabaseEntities(final boolean includeSystemKeyspaces) {
        final Multimap<String, String> keyspaceAndTables = HashMultimap.create();
        final List<String> keyspaces = new ArrayList<>();

        for (final Entry<String, Keyspace> keyspace : snapshot.getKeyspaces().entrySet()) {
            if (KeyspaceTable.isSystemKeyspace(keyspace.getKey()) && !includeSystemKeyspaces) {
                continue;
            }
            for (final Entry<String, Table> table : keyspace.getValue().getTables().entrySet()) {
                keyspaceAndTables.put(keyspace.getKey(), table.getValue().name);
            }

            keyspaces.add(keyspace.getKey());
        }

        return new DatabaseEntities(keyspaces, keyspaceAndTables);
    }

    @JsonIgnore
    public List<ManifestEntry> getManifestEntries() {
        return getManifestEntries(true);
    }

    @JsonIgnore
    public long getTotalSize() {
        return getManifestEntries(false).stream().map(e -> e.size).reduce(Long::sum).orElse(0L);
    }

    @JsonIgnore
    public List<ManifestEntry> getManifestEntries(final boolean withManifestItself) {
        final List<ManifestEntry> entries = new ArrayList<>();

        snapshot.getKeyspaces().forEach((s, keyspace) -> keyspace.getTables().forEach((s1, table) -> entries.addAll(table.getEntries())));

        if (withManifestItself && manifest != null) {
            entries.add(manifest);
        }

        return entries;
    }

    @JsonIgnore
    public String getManifestName() {
        if (manifest != null && manifest.objectKey != null) {
            final String manifestName = manifest.objectKey.getFileName().toString();
            // dot is file type separator
            return manifestName.substring(0, manifestName.lastIndexOf("."));
        }

        return null;
    }

    @JsonIgnore
    public Long getManifestTimestamp() {
        if (manifest != null && manifest.objectKey != null) {
            final String manifestPath = manifest.objectKey.getFileName().toString();
            final String timestampWithFileSuffix = manifestPath.substring(manifestPath.lastIndexOf("-") + 1);
            final String timestamp = timestampWithFileSuffix.substring(0, timestampWithFileSuffix.lastIndexOf("."));
            return Long.parseLong(timestamp);
        }

        return null;
    }

    public void cleanup() throws Exception {
        if (manifest != null) {
            Files.deleteIfExists(manifest.localFile);
        }
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final Manifest manifest1 = (Manifest) o;
        return Objects.equal(snapshot, manifest1.snapshot) &&
            Objects.equal(manifest, manifest1.manifest) &&
            Objects.equal(tokens, manifest1.tokens) &&
            Objects.equal(schemaVersion, manifest1.schemaVersion);
    }

    @Override
    public Manifest clone() throws CloneNotSupportedException {
        final Manifest cloned = new Manifest();

        cloned.setTokens(tokens == null ? null : new ArrayList<>(tokens));
        cloned.setSchemaVersion(this.schemaVersion);
        cloned.setManifest(manifest == null ? null : manifest.clone());
        cloned.setSnapshot(snapshot == null ? null : snapshot.clone());

        return cloned;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(snapshot, manifest, tokens, schemaVersion);
    }

    public static ManifestEntry getManifestAsManifestEntry(final Path localManifestPath,
                                                           BaseBackupOperationRequest request) {
        return new ManifestEntry(Paths.get("manifests").resolve(localManifestPath.getFileName()),
                                 localManifestPath,
                                 MANIFEST_FILE,
                                 null,
                                 request.kmsKeyId);
    }

    public static void write(final Manifest manifest, final Path localManifestPath, final ObjectMapper objectMapper) throws Exception {
        Files.createDirectories(localManifestPath.getParent());
        objectMapper.writeValue(localManifestPath.toFile(), manifest);
    }

    public static String write(final Manifest manifest, final ObjectMapper objectMapper) throws Exception {
        return objectMapper.writeValueAsString(manifest);
    }

    public static Manifest read(final Path localManifestPath, final ObjectMapper objectMapper) throws Exception {
        return objectMapper.readValue(localManifestPath.toFile(), Manifest.class);
    }

    public static Manifest read(final String manifest, final ObjectMapper objectMapper) throws Exception {
        return objectMapper.readValue(manifest, Manifest.class);
    }

    public static Path getLocalManifestPath(final String snapshotTag) {
        return Paths.get(System.getProperty("java.io.tmpdir", "/tmp")).resolve("manifests").resolve(snapshotTag + ".json");
    }

    public static synchronized String parseLatestManifest(final List<String> manifestsPaths) {
        // manifests/snapshot-uuid-timestamp

        String latestManifest = null;
        long latestTimestamp = 0;

        for (final String manifestPath : manifestsPaths) {
            try {

                final String timestampWithFileSuffix = manifestPath.substring(manifestPath.lastIndexOf("-") + 1);
                final String timestamp = timestampWithFileSuffix.substring(0, timestampWithFileSuffix.lastIndexOf("."));

                final long currentTimestamp = Long.parseLong(timestamp);
                if (currentTimestamp > latestTimestamp) {
                    latestTimestamp = currentTimestamp;
                    latestManifest = manifestPath;
                }
            } catch (final NumberFormatException ex) {
                logger.warn(format("Could not parse timestamp from manifest path %s", manifestPath), ex);
            }
        }

        if (latestManifest == null) {
            throw new IllegalStateException(format("Could not parse latest manifest from %s", manifestsPaths));
        }

        logger.info("Resolved manifest: {}", latestManifest);

        return latestManifest;
    }

    // helpers

    @JsonIgnore
    // Called in InPlaceStrategy only
    public List<ManifestEntry> getManifestFiles(final DatabaseEntities entities,
                                                final boolean restoreSystemKeyspace,
                                                final boolean restoreSystemAuth,
                                                final boolean newCluster,
                                                final boolean withSchemas) {
        final List<ManifestEntry> manifestEntries = new ArrayList<>();

        if (entities.areEmpty()) {
            for (final Entry<String, Keyspace> entry : snapshot.getKeyspaces().entrySet()) {
                manifestEntries.addAll(entry.getValue().getManifestEntries());
            }
        } else if (entities.tableSubsetOnly()) {
            for (final Entry<String, String> entry : entities.getKeyspacesAndTables().entries()) {

                final Optional<Keyspace> keyspace = snapshot.getKeyspace(entry.getKey());

                if (keyspace.isPresent()) {
                    final Optional<Table> table = keyspace.get().getTable(entry.getValue());
                    table.ifPresent(value -> manifestEntries.addAll(value.getEntries()));
                }
            }
        } else {
            for (final String ks : entities.getKeyspaces()) {
                final Optional<Keyspace> keyspace = snapshot.getKeyspace(ks);

                //throw new IllegalStateException(format("Keyspace %s is not in manifest!", ks));
                keyspace.ifPresent(value -> manifestEntries.addAll(value.getManifestEntries()));
            }
        }

        Stream<ManifestEntry> manifestEntryStream = manifestEntries.stream();

        if (!withSchemas) {
            manifestEntryStream = manifestEntryStream.filter(entry -> entry.type != Type.CQL_SCHEMA);
        }

        // take care or system keyspaces
        manifestEntryStream = manifestEntryStream.filter(entry -> {
            final String keyspace = entry.keyspaceTable.keyspace;
            final String table = entry.keyspaceTable.table;

            if (!KeyspaceTable.isSystemKeyspace(keyspace)) {
                return true;
            }

            // schemas are required only for cases we are going to restore into vanilla node but we have already
            // some data in tables so we need to tell Cassandra what tables we got
            // there is --restore-into-new-cluster flag telling that (translates to "newCluster")

            // Cassandra 2.0 case
            if (keyspace.equals("system")) {
                if (table.startsWith("schema_")) {
                    // if we restore into new cluster, we want to restore it
                    // if we do not, we ask if we want to restore system keyspaces as such
                    return newCluster || restoreSystemKeyspace;
                }
            }

            // Cassandra 3+ case
            if (keyspace.equals("system_schema")) {
                return newCluster || restoreSystemKeyspace;
            }

            if (keyspace.equals("system_auth")) {
                return restoreSystemAuth || restoreSystemKeyspace;
            }

            // if it is some other system keyspace / table, which is not necessary for restorations from scratch,
            // we just ask if we want to indeed restore it, this method is called in InPlace strategy only
            return restoreSystemKeyspace;
        });

        return manifestEntryStream.collect(toList());
    }

    // for in place strategy
    public void enrichManifestEntries() {

        snapshot.getKeyspaces().forEach((ksName, keyspace) -> {
            keyspace.getTables().forEach((tableName, table) -> {
                table.getEntries().forEach(entry -> {
                    final Path objectKey = entry.objectKey;
                    final int hashPathPart = SSTableUtils.isSecondaryIndexManifest(objectKey) ? 4 : 3;
                    // localFile will be here, for example, "keyspace/table-with-id/me-5-big-Data.db"
                    entry.localFile = objectKey.subpath(1, hashPathPart).resolve(objectKey.getFileName());
                    entry.keyspaceTable = new KeyspaceTable(ksName, tableName);
                });
            });
        });
    }

    public static class ManifestAgeComparator implements Comparator<String> {

        private Long extractTimestamp(String manifestPath) {
            final String timestampWithFileSuffix = manifestPath.substring(manifestPath.lastIndexOf("-") + 1);
            final String timestamp = timestampWithFileSuffix.substring(0, timestampWithFileSuffix.lastIndexOf("."));
            return Long.parseLong(timestamp);
        }

        @Override
        public int compare(final String manifestPath1, final String manifestPath2) {
            final long t1 = extractTimestamp(manifestPath1);
            final long t2 = extractTimestamp(manifestPath2);
            // older at the bottom, newer at top
            return (int) (t2 - t1);
        }
    }

    public static class ManifestAgePathComparator implements Comparator<Path> {

        private final ManifestAgeComparator comparator = new ManifestAgeComparator();

        @Override
        public int compare(final Path o1, final Path o2) {
            return comparator.compare(o1.getFileName().toString(), o2.getFileName().toString());
        }
    }

    public static class ManifestFilesCounter {

        public final Map<String, List<String>> files = new ConcurrentHashMap<>();
        public final Map<String, Long> sizes = new ConcurrentHashMap<>();

        public List<String> getManifestsOfEntry(final String manifestEntry) {
            return files.get(manifestEntry);
        }

        public boolean isOnlyInOneManifest(final String manifestEntry) {
            return files.entrySet().stream().filter(entry -> entry.getKey().equals(manifestEntry)).findFirst().map(e -> e.getValue().size()).orElse(0) == 1;
        }

        public boolean isInMultipleManifests(final String manifestEntry) {
            return files.entrySet().stream().filter(entry -> entry.getKey().equals(manifestEntry)).findFirst().map(e -> e.getValue().size()).orElse(0) > 1;
        }

        public int count(final String manifestEntry) {
            return files.entrySet().stream()
                .filter(entry -> entry.getKey().equals(manifestEntry))
                .map(entry -> entry.getValue().size())
                .reduce(Integer::sum).orElse(0);
        }

        public void add(final String manifestName, final ManifestEntry manifestEntry) {
            final String key = manifestEntry.objectKey.toString();
            if (files.containsKey(key)) {
                files.get(key).add(manifestName);
            } else {
                files.put(key, new ArrayList<String>() {{
                    add(manifestName);
                }});
                // we put it into sizes map just once
                // as it might be technically present in all manifests multiple times
                // but it is persisted just once
                sizes.put(key, manifestEntry.size);
            }
        }

        public void add(final Manifest manifest) {
            final String manifestName = manifest.manifest.objectKey.getFileName().toString();
            // with schemas but without manifest itself
            manifest.getManifestEntries(false).forEach(m -> add(manifestName, m));
        }

        public int getNumberOfEntries() {
            return files.keySet().size();
        }

        public long getSize() {
            return sizes.values().stream().reduce(Long::sum).orElse(0L);
        }

        public long getReclaimableSpace(final Manifest m) {
            long reclaimableSpace = 0;

            for (final ManifestEntry manifestEntry : m.getManifestEntries()) {
                final String key = manifestEntry.objectKey.toString();
                if (isOnlyInOneManifest(key)) {
                    reclaimableSpace += sizes.getOrDefault(key, 0L);
                }
            }

            return reclaimableSpace;
        }

        public List<String> getRemovableEntries(final Manifest m) {
            final List<String> removableEntries = new ArrayList<>();
            for (final ManifestEntry manifestEntry : m.getManifestEntries()) {
                final String key = manifestEntry.objectKey.toString();
                if (isOnlyInOneManifest(key)) {
                    removableEntries.add(manifestEntry.objectKey.toString());
                }
            }

            return removableEntries;
        }
    }

    public static class ManifestReporter {

        public ManifestReport report(final Manifest manifest) {
            final ManifestReport report = new ManifestReport();
            report.files = manifest.getManifestEntries(false).size();
            report.size = manifest.getTotalSize();
            report.name = manifest.getManifestName();
            report.manifest = manifest.manifest;
            return report;
        }

        public static class ManifestReport {

            public int files;
            @JsonSerialize(using = ConditionalHumanUnitsSerializer.class)
            public long size;
            public String name;
            @JsonSerialize(using = ConditionalHumanUnitsSerializer.class)
            public long reclaimableSpace;
            public List<String> removableEntries = new ArrayList<>();
            public String timestamp;
            public ManifestEntry manifest;
            public Long unixtimestamp;

            public int getFiles() {
                return files;
            }

            public void setFiles(final int files) {
                this.files = files;
            }

            public long getSize() {
                return size;
            }

            public void setSize(final long size) {
                this.size = size;
            }

            public long getReclaimableSpace() {
                return reclaimableSpace;
            }

            public void setReclaimableSpace(final long reclaimableSpace) {
                this.reclaimableSpace = reclaimableSpace;
            }

            public String getName() {
                return name;
            }

            public void setName(final String name) {
                this.name = name;
            }

            public List<String> getRemovableEntries() {
                return removableEntries;
            }

            public void setRemovableEntries(final List<String> removableEntries) {
                this.removableEntries = removableEntries;
            }

            public String getTimestamp() {
                return timestamp;
            }

            public void setTimestamp(final String timestamp) {
                this.timestamp = timestamp;
            }

            public Long getUnixtimestamp() {
                return unixtimestamp;
            }

            public void setUnixtimestamp(final Long unixtimestamp) {
                this.unixtimestamp = unixtimestamp;
            }

            public ManifestEntry getManifest() {
                return manifest;
            }

            public void setManifest(final ManifestEntry manifest) {
                this.manifest = manifest;
            }

            @Override
            public String toString() {
                return MoreObjects.toStringHelper(this)
                    .add("name", name)
                    .add("files", files)
                    .add("size", size)
                    .add("reclaimableSpace", reclaimableSpace)
                    .add("removableEntries", removableEntries)
                    .add("timestamp", timestamp)
                    .add("unixtimestamp", unixtimestamp)
                    .toString();
            }
        }
    }

    public static class ConditionalHumanUnitsSerializer extends JsonSerializer<Long> {

        public static final String HUMAN_UNITS_SERIALISATION_PROPERTY = "humanUnits";

        @Override
        public void serialize(Long value, JsonGenerator jsonGenerator, SerializerProvider serializerProvider) throws IOException {
            if (value == null) {
                jsonGenerator.writeNull();
                return;
            }

            Object attribute = serializerProvider.getAttribute(HUMAN_UNITS_SERIALISATION_PROPERTY);

            if (attribute != null && (boolean) attribute) {
                jsonGenerator.writeString(humanReadableByteCountSI(value));
            } else {
                jsonGenerator.writeNumber(value);
            }
        }

        public static String humanReadableByteCountSI(long bytes) {
            if (-1000 < bytes && bytes < 1000) {
                return bytes + " B";
            }
            CharacterIterator ci = new StringCharacterIterator("kMGTPE");
            while (bytes <= -999_950 || bytes >= 999_950) {
                bytes /= 1000;
                ci.next();
            }
            return String.format("%.1f %cB", bytes / 1000.0, ci.current());
        }
    }

    public static class AllManifestsReport {

        @JsonSerialize(using = ConditionalHumanUnitsSerializer.class)
        public long totalSize;
        public int totalFiles;
        public int totalManifests;
        public List<ManifestReport> reports = new ArrayList<>();

        public long getTotalSize() {
            return totalSize;
        }

        public void setTotalSize(final long totalSize) {
            this.totalSize = totalSize;
        }

        public int getTotalFiles() {
            return totalFiles;
        }

        public void setTotalFiles(final int totalFiles) {
            this.totalFiles = totalFiles;
        }

        public int getTotalManifests() {
            return totalManifests;
        }

        public void setTotalManifests(final int totalManifests) {
            this.totalManifests = totalManifests;
        }

        public List<ManifestReport> getReports() {
            return reports;
        }

        public void setReports(final List<ManifestReport> reports) {
            this.reports = reports;
        }

        @JsonIgnore
        public Optional<ManifestReport> getLatest() {
            return reports.isEmpty() ? Optional.empty() : Optional.of(reports.get(0));
        }

        @JsonIgnore
        public Optional<ManifestReport> getOldest() {
            return reports.isEmpty() ? Optional.empty() : Optional.of(reports.get(reports.size() - 1));
        }

        public Optional<ManifestReport> get(final String name) {
            return reports.stream().filter(m -> m.name.equals(name)).findFirst();
        }

        public static AllManifestsReport report(List<Manifest> manifests) {
            final ManifestFilesCounter counter = new ManifestFilesCounter();
            final ManifestReporter manifestReporter = new ManifestReporter();
            final List<ManifestReport> reports = new ArrayList<>();

            for (final Manifest m : manifests) {
                counter.add(m);
            }

            for (final Manifest m : manifests) {
                final ManifestReport report = manifestReporter.report(m);
                report.reclaimableSpace = counter.getReclaimableSpace(m);
                report.removableEntries = counter.getRemovableEntries(m);
                Long manifestTimestamp = m.getManifestTimestamp();
                report.timestamp = new Timestamp(m.getManifestTimestamp()).toLocalDateTime().toString();
                report.unixtimestamp = manifestTimestamp;
                reports.add(report);
            }

            final AllManifestsReport report = new AllManifestsReport();
            report.totalFiles = counter.getNumberOfEntries();
            report.totalManifests = reports.size();
            report.totalSize = counter.getSize();
            report.reports = reports;

            return report;
        }

        public List<ManifestReport> filter(final Predicate<ManifestReport> predicate) {
            return reports.stream().filter(predicate).collect(toList());
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                .add("totalSize", totalSize)
                .add("totalFiles", totalFiles)
                .add("totalManifests", totalManifests)
                .add("reports", reports)
                .toString();
        }
    }
}
