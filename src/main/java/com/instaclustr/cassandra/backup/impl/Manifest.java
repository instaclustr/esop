package com.instaclustr.cassandra.backup.impl;

import static com.instaclustr.cassandra.backup.impl.ManifestEntry.Type.CQL_SCHEMA;
import static java.util.stream.Collectors.toList;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Objects;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.instaclustr.cassandra.backup.impl.Snapshots.Snapshot;
import com.instaclustr.cassandra.backup.impl.Snapshots.Snapshot.Keyspace;
import com.instaclustr.cassandra.backup.impl.Snapshots.Snapshot.Keyspace.Table;

public class Manifest implements Cloneable {

    public void filter(final DatabaseEntities entities) {

    }

    private Snapshot snapshot;

    @JsonIgnore
    private ManifestEntry manifest;

    private List<String> tokens;

    private String schemaVersion;

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

    public void enrichManifestEntries(final Path localPathRoot) {

        snapshot.getKeyspaces().forEach((ksName, keyspace) -> {
            keyspace.getTables().forEach((tableName, table) -> {
                table.getEntries().forEach(entry -> {
                    final Path objectKey = entry.objectKey;
                    final int hashPathPart = SSTableUtils.isSecondaryIndexManifest(objectKey) ? 4 : 3;
                    entry.localFile = localPathRoot.resolve(objectKey.subpath(0, hashPathPart)).resolve(objectKey.getFileName());
                });
            });
        });
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
        return getManifestEntries(true, true);
    }

    @JsonIgnore
    public List<ManifestEntry> getManifestEntries(final boolean withSchemas, final boolean withManifestItself) {
        final List<ManifestEntry> entries = new ArrayList<>();

        snapshot.getKeyspaces().forEach((s, keyspace) -> keyspace.getTables().forEach((s1, table) -> {
            if (withSchemas) {
                // it contains schema already
                entries.addAll(table.getEntries());
            } else {
                entries.addAll(table.getEntries().stream().filter(entry -> entry.type != CQL_SCHEMA).collect(toList()));
            }
        }));

        if (withManifestItself && manifest != null) {
            entries.add(manifest);
        }

        return entries;
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

    public static ManifestEntry getManifestAsManifestEntry(final Path localManifestPath) throws Exception {
        return new ManifestEntry(Paths.get("manifests").resolve(localManifestPath.getFileName()), localManifestPath, ManifestEntry.Type.MANIFEST_FILE);
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

    public static Path getLocalManifestPath(final Path cassandraDir, final String snapshotTag) {
        return cassandraDir.resolve("manifests").resolve(snapshotTag + ".json");
    }

    public static Set<Path> getLocalExistingEntries(final Path dataDir) {
        final Set<Path> existingEntries = new HashSet<>();
        final int skipBackupsAndSnapshotsFolders = 4;

        if (dataDir.toFile().exists()) {
            try (Stream<Path> paths = Files.walk(dataDir, skipBackupsAndSnapshotsFolders)) {
                paths.filter(Files::isRegularFile).forEach(existingEntries::add);
            } catch (final IOException ex) {
                throw new IllegalStateException(String.format("Unable to walk through Cassandra data dir %s", dataDir), ex);
            }
        }

        return existingEntries;
    }

    // helpers

    @JsonIgnore
    public List<ManifestEntry> getManifestFiles(final DatabaseEntities entities,
                                                final boolean restoreSystemKeyspace,
                                                final boolean newCluster) {
        return getManifestFiles(entities, restoreSystemKeyspace, newCluster, true);
    }

    private boolean filterSystemKeyspace(String ks, boolean restoreSystemKeyspace, boolean newCluster) {
        if (KeyspaceTable.isSystemKeyspace(ks)) {
            if (KeyspaceTable.isBootstrappingKeyspace(ks)) {
                return !newCluster && !restoreSystemKeyspace;
            } else {
                return !restoreSystemKeyspace;
            }
        }

        return false;
    }

    @JsonIgnore
    public List<ManifestEntry> getManifestFiles(final DatabaseEntities entities,
                                                final boolean restoreSystemKeyspace,
                                                final boolean newCluster,
                                                final boolean withSchemas) {

        final List<ManifestEntry> manifestEntries = new ArrayList<>();

        if (entities.areEmpty()) {
            for (final Entry<String, Keyspace> entry : snapshot.getKeyspaces().entrySet()) {
                if (KeyspaceTable.isSystemKeyspace(entry.getKey()) && !restoreSystemKeyspace) {
                    continue;
                }

                manifestEntries.addAll(entry.getValue().getManifestEntries());
            }
        } else if (entities.tableSubsetOnly()) {
            for (final Entry<String, String> entry : entities.getKeyspacesAndTables().entries()) {

                final Optional<Keyspace> keyspace = snapshot.getKeyspace(entry.getKey());

                if (!keyspace.isPresent()) {
                    continue;
                }

                if (filterSystemKeyspace(entry.getKey(), restoreSystemKeyspace, newCluster)) {
                    continue;
                }

                final Optional<Table> table = keyspace.get().getTable(entry.getValue());

                table.ifPresent(value -> manifestEntries.addAll(value.getEntries()));
            }
        } else {
            for (final String ks : entities.getKeyspaces()) {
                final Optional<Keyspace> keyspace = snapshot.getKeyspace(ks);

                if (!keyspace.isPresent()) {
                    continue;
                }

                if (filterSystemKeyspace(ks, restoreSystemKeyspace, newCluster)) {
                    continue;
                }

                manifestEntries.addAll(keyspace.get().getManifestEntries());
            }
        }

        if (!withSchemas) {
            return manifestEntries.stream().filter(entry -> entry.type != CQL_SCHEMA).collect(toList());
        }

        return manifestEntries;
    }
}
