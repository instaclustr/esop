package com.instaclustr.esop.impl.restore.strategy;

import com.instaclustr.esop.impl.CassandraData;
import com.instaclustr.esop.impl.DatabaseEntities;
import com.instaclustr.esop.impl.Manifest;
import com.instaclustr.esop.impl.ManifestEntry;
import com.instaclustr.esop.impl.SSTableUtils;
import com.instaclustr.esop.impl.restore.RestoreOperationRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.instaclustr.io.FileUtils.cleanDirectory;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toList;

public class DataSynchronizator {
    private static final Logger logger = LoggerFactory.getLogger(DataSynchronizator.class);
    private final Manifest manifest;
    private final RestoreOperationRequest request;
    private final List<ManifestEntry> entriesToDownload = new ArrayList<>();
    private final List<Path> filesToDelete = new ArrayList<>();

    public DataSynchronizator(final Manifest manifest,
                              final RestoreOperationRequest request) {
        this.manifest = manifest;
        this.request = request;
    }

    public DataSynchronizator execute() {
        final DatabaseEntities filteredManifestDatabaseEntities = manifest.getDatabaseEntities(true).filter(request.entities);

        logger.info("filtered entities from manifest: " + filteredManifestDatabaseEntities.toString());

        final List<ManifestEntry> entriesFromManifest = manifest.getManifestFiles(filteredManifestDatabaseEntities,
                                                                                  request.restoreSystemKeyspace,
                                                                                  request.restoreSystemAuth,
                                                                                  request.newCluster,
                                                                                  true);

        // 3. Build a list of all SSTables files currently present (that are candidates for deleting)
        final Set<Path> localDataFiles = CassandraData.getLocalDataFiles(request.dirs.data());

        logger.info("Restoring to existing cluster: {}", localDataFiles.size() > 0);

        // the first round, see what is in manifest and what is currently present,
        // if it is not present, we will download it

        for (final ManifestEntry entryFromManifest : entriesFromManifest) {
            // do not download schemas
            if (entryFromManifest.type == ManifestEntry.Type.CQL_SCHEMA) {
                continue;
            }

            if (CassandraData.containsFile(localDataFiles, entryFromManifest.localFile)) {
                // this file exists on a local disk as well as in manifest, there is nothing to download nor remove
                logger.info(String.format("%s found locally, not downloading", entryFromManifest.localFile));
            } else {
                // if it does not exist locally, we have to download it
                entriesToDownload.add(entryFromManifest);
            }
        }

        // the second round, see what is present locally, but it is not in the manifest
        // if it is not in manifest, we need to delete it,
        // otherwise we compare hashes, if they do not match, we delete as well

        for (final Path localExistingFile : localDataFiles) {
            final Optional<ManifestEntry> first = entriesFromManifest.stream().filter(me -> localExistingFile.endsWith(me.localFile)).findFirst();

            if (first.isPresent()) {
                // if it exists, hash has to be same, otherwise delete it
                if (!SSTableUtils.isExistingSStable(localExistingFile,
                                                    first.get().objectKey.getName(SSTableUtils.isSecondaryIndexManifest(first.get().objectKey) ? 4 : 3).toString())) {
                    filesToDelete.add(localExistingFile);
                }
            } else {
                filesToDelete.add(localExistingFile);
            }
        }

        return this;
    }

    public static abstract class SSTableClassifier<ENTRY_TYPE> {

        // some/path/keyspace/tableId/me-1-big-Data.db
        private static final Pattern p = Pattern.compile("(.*)/(.*)/" + SSTableUtils.SSTABLE_RE.pattern());

        public String getSimplePath(ENTRY_TYPE entry) {
            String path = getPath(entry);
            final Matcher matcher = p.matcher(path);
            if (matcher.matches()) {
                String g1 = matcher.group(1);
                String g2 = matcher.group(2);
                String g3 = matcher.group(3);
                if (g2.startsWith(".")) {
                    return g1 + "/" + g3;
                } else {
                    return g1 + "/" + g2 + "/" + g3;
                }
            } else {
                return "";
            }
        }

        public abstract String getPath(ENTRY_TYPE entry);

        public abstract List<ENTRY_TYPE> mapping(List<ENTRY_TYPE> entries, Path path);

        public Map<String, List<ENTRY_TYPE>> classify(final List<ENTRY_TYPE> entries) {
            final Map<String, List<ENTRY_TYPE>> classified = entries
                    .stream()
                    .collect(Collectors.groupingBy(this::getSimplePath,
                                                   LinkedHashMap::new,
                                                   Collectors.mapping(identity(), toList())))
                    .entrySet()
                    .stream()
                    .filter(entry -> !entry.getKey().equals(""))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (x, y) -> y, LinkedHashMap::new));

            return classified;
        }

        public void map(final Map<String, List<ENTRY_TYPE>> classified,
                        final RestoreOperationRequest request) {
            Iterator<Map.Entry<String, List<ENTRY_TYPE>>> iterator = classified.entrySet().iterator();

            int sstableNumber = 0;
            int numberOfDataDirs = request.dataDirs.size();

            while (iterator.hasNext()) {
                final Map.Entry<String, List<ENTRY_TYPE>> next = iterator.next();
                final Path dataDir = request.dataDirs.get(sstableNumber % numberOfDataDirs);
                List<ENTRY_TYPE> mapping = mapping(next.getValue(), dataDir);
                next.setValue(mapping);
                ++sstableNumber;
            }
        }
    }

    public static class ManifestEntrySSTableClassifier extends SSTableClassifier<ManifestEntry> {
        @Override
        public String getPath(ManifestEntry entry) {
            if (entry.localFile == null) {
                // objectKey is "data/keyspace/table/gen-hash/entry.db
                // and we want "keyspace/table/entry.db"
                final int hashPathPart = SSTableUtils.isSecondaryIndexManifest(entry.objectKey) ? 4 : 3;
                return entry.objectKey.subpath(1, hashPathPart).resolve(entry.objectKey.getFileName()).toString();
            } else {
                return entry.localFile.toString();
            }
        }

        @Override
        public List<ManifestEntry> mapping(List<ManifestEntry> entries, Path path) {
            for (final ManifestEntry entry : entries) {
                entry.localFile = path.resolve(entry.localFile);
            }
            return entries;
        }
    }

    public static class PathSSTableClassifier extends SSTableClassifier<Path> {

        private final RestoreOperationRequest request;

        public PathSSTableClassifier(final RestoreOperationRequest request) {
            this.request = request;
        }

        @Override
        public String getPath(Path entry) {
            return request.importing.sourceDir.relativize(entry).toString();
        }

        @Override
        public List<Path> mapping(List<Path> entries, Path path) {
            return entries.stream().map(entry -> path.resolve(request.importing.sourceDir.relativize(entry))).collect(toList());
        }
    }

    public List<ManifestEntry> entriesToDownload() {
        return entriesToDownload;
    }

    public List<Path> filesToDelete() {
        return filesToDelete;
    }

    public void deleteUnnecessarySSTableFiles() {
        // 5. Delete any entries left in existingSstableList
        filesToDelete().forEach(sstablePath -> {
            logger.info("Deleting existing sstable {}", sstablePath);
            if (!sstablePath.toFile().delete()) {
                logger.warn("Failed to delete {}", sstablePath);
            }
        });
    }

    public void cleanData() {
        // 6. Clean out old data
        cleanDirectory(request.dirs.hints());
        cleanDirectory(request.dirs.savedCaches());
        cleanDirectory(request.dirs.commitLogs());
    }
}
