package com.instaclustr.esop.impl.restore.strategy;

import static com.instaclustr.io.FileUtils.cleanDirectory;
import static java.lang.String.format;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.instaclustr.esop.guice.BucketServiceFactory;
import com.instaclustr.esop.impl.AbstractTracker.Session;
import com.instaclustr.esop.impl.BucketService;
import com.instaclustr.esop.impl.DatabaseEntities;
import com.instaclustr.esop.impl.Manifest;
import com.instaclustr.esop.impl.ManifestEntry;
import com.instaclustr.esop.impl.ManifestEntry.Type;
import com.instaclustr.esop.impl.SSTableUtils;
import com.instaclustr.esop.impl.StorageLocation;
import com.instaclustr.esop.impl.restore.DownloadTracker;
import com.instaclustr.esop.impl.restore.DownloadTracker.DownloadUnit;
import com.instaclustr.esop.impl.restore.RestorationStrategy;
import com.instaclustr.esop.impl.restore.RestorationUtilities;
import com.instaclustr.esop.impl.restore.RestoreOperationRequest;
import com.instaclustr.esop.impl.restore.Restorer;
import com.instaclustr.esop.topology.CassandraClusterTopology.ClusterTopology;
import com.instaclustr.esop.topology.CassandraClusterTopology.ClusterTopology.NodeTopology;
import com.instaclustr.io.FileUtils;
import com.instaclustr.kubernetes.KubernetesHelper;
import com.instaclustr.operations.Operation;
import com.instaclustr.operations.Operation.Error;
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

    private final ObjectMapper objectMapper;
    private final DownloadTracker downloadTracker;
    private final Map<String, BucketServiceFactory> bucketServiceFactoryMap;

    @Inject
    public InPlaceRestorationStrategy(final ObjectMapper objectMapper,
                                      final DownloadTracker downloadTracker,
                                      final Map<String, BucketServiceFactory> bucketServiceFactoryMap) {
        this.objectMapper = objectMapper;
        this.downloadTracker = downloadTracker;
        this.bucketServiceFactoryMap = bucketServiceFactoryMap;
    }

    @Override
    public void restore(final Restorer restorer, final Operation<RestoreOperationRequest> operation) throws Exception {

        final RestoreOperationRequest request = operation.request;

        try {
            if (operation.request.restorationStrategyType != RestorationStrategyType.IN_PLACE) {
                throw new IllegalStateException(format("restorationStrategyType has to be of type '%s' in case you want to use %s, it is of type '%s'",
                                                       RestorationStrategyType.IN_PLACE,
                                                       InPlaceRestorationStrategy.class.getName(),
                                                       operation.request.restorationStrategyType));
            }

            // 0. check that bucket is there

            if (!request.skipBucketVerification) {
                try (final BucketService bucketService = bucketServiceFactoryMap.get(request.storageLocation.storageProvider).createBucketService(request)) {
                    bucketService.checkBucket(request.storageLocation.bucket, false);
                }
            }

            // 1. Resolve node to restore to

            if (operation.request.resolveHostIdFromTopology) {
                final NodeTopology nodeTopology = getNodeTopology(restorer, request);
                // here, nodeTopology.nodeId is uuid, not hostname
                operation.request.storageLocation = StorageLocation.updateNodeId(operation.request.storageLocation, nodeTopology.nodeId);
                restorer.updateStorageLocation(operation.request.storageLocation);
                logger.info(format("Updated storage location to %s", operation.request.storageLocation));
            }

            // 2. Download the manifest & tokens, chance to error out soon before we actually pull big files if something goes wrong here
            logger.info("Retrieving manifest for snapshot: {}", request.snapshotTag);

            final Manifest manifest = RestorationUtilities.downloadManifest(operation.request, restorer, null, objectMapper);
            manifest.enrichManifestEntries(request.cassandraDirectory);
            final DatabaseEntities filteredManifestDatabaseEntities = manifest.getDatabaseEntities(true).filter(request.entities,
                                                                                                                request.restoreSystemKeyspace,
                                                                                                                request.newCluster);

            final List<ManifestEntry> manifestFiles = manifest.getManifestFiles(filteredManifestDatabaseEntities,
                                                                                request.restoreSystemKeyspace,
                                                                                request.newCluster,
                                                                                true,
                                                                                operation.request.cassandraVersion);

            // 3. Build a list of all SSTables currently present, that are candidates for deleting
            final Set<Path> existingFiles = Manifest.getLocalExistingEntries(request.dirs.data());

            final List<ManifestEntry> entriesToDownload = new ArrayList<>();
            final List<Path> filesToDelete = new ArrayList<>();

            logger.info("Restoring to existing cluster: {}", existingFiles.size() > 0);

            // the first round, see what is in manifest and what is currently present,
            // if it is not present, we will download it

            for (final ManifestEntry manifestFile : manifestFiles) {
                // do not download schemas
                if (manifestFile.type == Type.CQL_SCHEMA) {
                    continue;
                }

                if (Files.exists(manifestFile.localFile)) {
                    // this file exists on a local disk as well as in manifest, there is nothing to download nor remove
                    logger.info(String.format("%s found locally, not downloading", manifestFile.localFile));
                } else {
                    // if it does not exist locally, we have to download it
                    entriesToDownload.add(manifestFile);
                }
            }

            // the second round, see what is present locally but it is not in the manifest
            // if it is not in the manifest, we need to remove it from disk

            for (final Path localExistingFile : existingFiles) {
                // if it is not in manifest

                final Optional<ManifestEntry> first = manifestFiles.stream().filter(me -> me.localFile.equals(localExistingFile)).findFirst();

                if (first.isPresent()) {
                    // if it exists, hash has to be same, otherwise delete it
                    if (!SSTableUtils.isExistingSStable(first.get().localFile, first.get().objectKey.getName(SSTableUtils.isSecondaryIndexManifest(first.get().objectKey) ? 4 : 3).toString())) {
                        filesToDelete.add(localExistingFile);
                    }
                } else {
                    filesToDelete.add(localExistingFile);
                }
            }

            // 4. Download files in the manifest

            Session<DownloadUnit> downloadSession = null;

            try {
                downloadSession = downloadTracker.submit(restorer, operation, entriesToDownload, operation.request.snapshotTag, operation.request.concurrentConnections);
                downloadSession.waitUntilConsideredFinished();
                downloadTracker.cancelIfNecessary(downloadSession);
            } finally {
                downloadTracker.removeSession(downloadSession);
            }

            // 5. Delete any entries left in existingSstableList
            filesToDelete.forEach(sstablePath -> {
                logger.info("Deleting existing sstable {}", sstablePath);
                if (!sstablePath.toFile().delete()) {
                    logger.warn("Failed to delete {}", sstablePath);
                }
            });

            // 6. Clean out old data
            cleanDirectory(request.dirs.hints());
            cleanDirectory(request.dirs.savedCaches());
            cleanDirectory(request.dirs.commitLogs());

            // 7.

            // K8S will handle copying over tokens.yaml fragment and disabling bootstrap fragment to right directory to be picked up by Cassandra
            // "standalone / vanilla" Cassandra installations has to cover this manually for now.
            // in the future, we might implement automatic configuration of cassandra.yaml for standalone installations
            if (request.updateCassandraYaml) {

                Path fileToAppendTo;
                boolean shouldAppend = true;

                if (KubernetesHelper.isRunningInKubernetes()) {
                    // Cassandra operator specific dir
                    fileToAppendTo = Paths.get("/var/lib/cassandra/tokens.yaml");
                } else {
                    fileToAppendTo = request.cassandraConfigDirectory.resolve("cassandra.yaml");
                    if (!Files.exists(fileToAppendTo)) {
                        logger.info(String.format("File %s does not exist, not going to append to it!", fileToAppendTo));
                        shouldAppend = false;
                    }
                }

                if (shouldAppend) {
                    FileUtils.replaceOrAppend(fileToAppendTo,
                                              content -> content.contains("auto_bootstrap: true"),
                                              content -> !content.contains("auto_bootstrap"),
                                              "auto_bootstrap: true",
                                              "auto_bootstrap: false");

                    if (FileUtils.contains(fileToAppendTo, "initial_token") && !FileUtils.contains(fileToAppendTo, "# initial_token")) {
                        logger.warn(String.format("%s file does already contain 'initial_token' property, this is unexpected and "
                                                      + "backup tooling is not going to update it for you, please proceed manually, new setting should be: %s",
                                                  fileToAppendTo,
                                                  manifest.getInitialTokensCassandraYamlFragment()));
                    } else {
                        FileUtils.appendToFile(fileToAppendTo, manifest.getInitialTokensCassandraYamlFragment());
                    }

                    logger.debug(String.format("Content of file %s to which necessary changes for restore were applied: ", fileToAppendTo));
                    logger.debug(new String(Files.readAllBytes(fileToAppendTo)));
                }
            } else {
                logger.info("Update of cassandra.yaml was turned off by --update-cassandra-yaml=false (or not specifying that flag at all.");
                logger.info("For the successful start of a node by Cassandra operator or manually, you have to do the following:");
                logger.info("1) add tokens in Cassandra installation dir to cassandra.yaml file");
                logger.info("2) change 'auto_bootstrap: true' to 'auto_bootstrap: false' in cassandra.yaml");
            }
        } catch (final Exception ex) {
            operation.addError(Error.from(ex));
        }
    }

    @Override
    public RestorationStrategyType getStrategyType() {
        return RestorationStrategyType.IN_PLACE;
    }

    private NodeTopology getNodeTopology(final Restorer restorer, final RestoreOperationRequest request) {
        try {
            final String topologyFile = format("topology/%s-%s", request.storageLocation.clusterId, request.snapshotTag);
            final String topology = restorer.downloadFileToString(Paths.get(topologyFile), fileName -> fileName.contains(topologyFile));
            final ClusterTopology clusterTopology = objectMapper.readValue(topology, ClusterTopology.class);
            // nodeId here is propagated by Cassandra operator and it is "hostname"
            // by translating, we get proper node id (uuid) so we fetch the right node in remote bucket
            return clusterTopology.translateToNodeTopology(request.storageLocation.nodeId);
        } catch (final Exception ex) {
            throw new IllegalStateException(format("Unable to resolve node hostId to restore to for request %s", request), ex);
        }
    }

}