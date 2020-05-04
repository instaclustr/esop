package com.instaclustr.cassandra.backup.impl.restore.strategy;

import static com.instaclustr.cassandra.backup.impl.restore.RestorationUtilities.getManifestFilesForFullExistingRestore;
import static com.instaclustr.cassandra.backup.impl.restore.RestorationUtilities.getManifestFilesForFullNewRestore;
import static com.instaclustr.cassandra.backup.impl.restore.RestorationUtilities.getManifestFilesForSubsetExistingRestore;
import static com.instaclustr.cassandra.backup.impl.restore.RestorationUtilities.getManifestFilesForSubsetNewRestore;
import static com.instaclustr.cassandra.backup.impl.restore.RestorationUtilities.isAnExistingSstable;
import static com.instaclustr.cassandra.backup.impl.restore.RestorationUtilities.isSecondaryIndexManifest;
import static com.instaclustr.cassandra.backup.impl.restore.RestorationUtilities.isSubsetTable;
import static com.instaclustr.io.FileUtils.cleanDirectory;
import static java.lang.String.format;
import static java.util.stream.Collectors.toList;

import java.io.BufferedReader;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import com.instaclustr.cassandra.backup.impl.ManifestEntry;
import com.instaclustr.operations.OperationProgressTracker;
import com.instaclustr.cassandra.backup.impl.restore.RestorationStrategy;
import com.instaclustr.cassandra.backup.impl.restore.RestorationUtilities.ManifestFilteringPredicate;
import com.instaclustr.cassandra.backup.impl.restore.RestorationUtilities.TokensFilteringPredicate;
import com.instaclustr.cassandra.backup.impl.restore.RestoreOperationRequest;
import com.instaclustr.cassandra.backup.impl.restore.Restorer;
import com.instaclustr.io.FileUtils;
import com.instaclustr.io.GlobalLock;
import com.instaclustr.operations.Operation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * In-place restoration strategy does not use phases because there is no need for it.
 * Whole restoration can be executed in one run.
 *
 * This strategy is expected to be run on stopped node only so we do not need to check its health either.
 *
 * This strategy is use in Cassandra operator upon the restoration of whole Cassandra cluster.
 */
public class InPlaceRestorationStrategy implements RestorationStrategy {

    private static final Logger logger = LoggerFactory.getLogger(InPlaceRestorationStrategy.class);

    @Override
    public void restore(final Restorer restorer, final Operation<RestoreOperationRequest> operation) throws Exception {

        final RestoreOperationRequest request = operation.request;

        final FileLock fileLock = new GlobalLock(request.lockFile).waitForLock();

        try {
            if (operation.request.restorationStrategyType != RestorationStrategyType.IN_PLACE) {
                throw new IllegalStateException(format("restorationStrategyType has to be of type '%s' in case you want to use %s, it is of type '%s'",
                                                       RestorationStrategyType.IN_PLACE,
                                                       InPlaceRestorationStrategy.class.getName(),
                                                       operation.request.restorationStrategyType));
            }

            // 2. Determine if just restoring a subset of tables
            final boolean isTableSubsetOnly = request.entities.tableSubsetOnly();

            // 3. Download the manifest & tokens, chance to error out soon before we actually pull big files if something goes wrong here
            logger.info("Retrieving manifest for snapshot: {}", request.snapshotTag);

            final Path localManifestPath = restorer.downloadFileToDir(request.cassandraDirectory.resolve("manifests"),
                                                                      Paths.get("manifests"),
                                                                      new ManifestFilteringPredicate(operation.request, null));

            final Path localTokensPath = restorer.downloadFileToDir(request.cassandraDirectory.resolve("tokens"),
                                                                    Paths.get("tokens"),
                                                                    new TokensFilteringPredicate(operation.request, null));

            // 4. Clean out old data
            cleanDirectory(request.cassandraDirectory.resolve("hints"));
            cleanDirectory(request.cassandraDirectory.resolve("saved_caches"));

            // 5. Build a list of all SSTables currently present, that are candidates for deleting
            final Set<Path> existingSstableList = new HashSet<>();
            final int skipBackupsAndSnapshotsFolders = 4;

            final Path cassandraSstablesDirectory = request.cassandraDirectory.resolve("data");

            if (cassandraSstablesDirectory.toFile().exists()) {
                try (Stream<Path> paths = Files.walk(cassandraSstablesDirectory, skipBackupsAndSnapshotsFolders)) {
                    if (isTableSubsetOnly) {
                        paths.filter(Files::isRegularFile)
                            .filter(isSubsetTable(request.entities))
                            .forEach(existingSstableList::add);
                    } else {
                        paths.filter(Files::isRegularFile).forEach(existingSstableList::add);
                    }
                }
            }

            final boolean isRestoringToExistingCluster = existingSstableList.size() > 0;
            logger.info("Restoring to existing cluster: {}", isRestoringToExistingCluster);

            // 5. Parse the manifest
            final LinkedList<ManifestEntry> downloadManifest = new LinkedList<>();

            try (final BufferedReader manifestStream = Files.newBufferedReader(localManifestPath)) {
                final List<String> filteredManifest;

                if (isRestoringToExistingCluster) {
                    if (isTableSubsetOnly) {
                        filteredManifest = manifestStream.lines()
                            .filter(getManifestFilesForSubsetExistingRestore(request.entities, request.restoreSystemKeyspace))
                            .collect(toList());
                    } else {
                        filteredManifest = manifestStream.lines()
                            .filter(getManifestFilesForFullExistingRestore(request.restoreSystemKeyspace))
                            .collect(toList());
                    }
                } else {
                    if (isTableSubsetOnly) {
                        filteredManifest = manifestStream.lines()
                            .filter(getManifestFilesForSubsetNewRestore(request.entities, request.restoreSystemKeyspace))
                            .collect(toList());
                    } else {
                        filteredManifest = manifestStream.lines()
                            .filter(getManifestFilesForFullNewRestore(request.restoreSystemKeyspace))
                            .collect(toList());
                    }
                }

                for (final String m : filteredManifest) {
                    final String[] lineArray = m.trim().split(" ");

                    final Path manifestPath = Paths.get(lineArray[1]);
                    final int hashPathPart = isSecondaryIndexManifest(manifestPath) ? 4 : 3;

                    //strip check hash from path
                    final Path localPath = request.cassandraDirectory.resolve(manifestPath.subpath(0, hashPathPart).resolve(manifestPath.getFileName()));

                    if (isAnExistingSstable(localPath, manifestPath.getName(hashPathPart).toString())) {
                        logger.info("Keeping existing sstable " + localPath);
                        existingSstableList.remove(localPath);
                        continue; // file already present, and the hash matches so don't add to manifest to download and don't delete
                    }

                    logger.debug("Not keeping existing sstable {}", localPath);
                    downloadManifest.add(new ManifestEntry(manifestPath, localPath, ManifestEntry.Type.FILE, 0, null));
                }
            }

            // 6. Delete any entries left in existingSstableList
            existingSstableList.forEach(sstablePath -> {
                logger.info("Deleting existing sstable {}", sstablePath);
                if (!sstablePath.toFile().delete()) {
                    logger.warn("Failed to delete {}", sstablePath);
                }
            });

            // 7. Download files in the manifest
            restorer.downloadFiles(downloadManifest, new OperationProgressTracker(operation, downloadManifest.size()));

            // K8S will handle copying over tokens.yaml fragment and disabling bootstrap fragment to right directory to be picked up by Cassandra
            // "standalone / vanilla" Cassandra installations has to cover this manually for now.
            // in the future, we might implement automatic configuration of cassandra.yaml for standalone installations
            if (request.updateCassandraYaml) {
                final Path cassandraYaml = request.cassandraConfigDirectory.resolve("cassandra.yaml");

                FileUtils.appendToFile(cassandraYaml, localTokensPath);
                FileUtils.replaceInFile(cassandraYaml, "auto_bootstrap: true", "auto_bootstrap: false");
            } else {
                logger.info("Update of cassandra.yaml was turned off by --update-cassandra-yaml=false (or not specifying that flag at all.");
                logger.info("For the successful start of a node, you have to do the following:");
                logger.info("1) add the content of the tokens.yaml in Cassandra installation dir to cassandra.yaml file");
                logger.info("2) change 'auto_bootstrap: true' to 'auto_bootstrap: false' in cassandra.yaml");
            }
        } finally {
            fileLock.release();
        }
    }

    @Override
    public RestorationStrategyType getStrategyType() {
        return RestorationStrategyType.IN_PLACE;
    }
}
