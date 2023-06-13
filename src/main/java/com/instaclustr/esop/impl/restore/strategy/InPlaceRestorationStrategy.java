package com.instaclustr.esop.impl.restore.strategy;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.instaclustr.esop.guice.BucketServiceFactory;
import com.instaclustr.esop.impl.AbstractTracker.Session;
import com.instaclustr.esop.impl.BucketService;
import com.instaclustr.esop.impl.Manifest;
import com.instaclustr.esop.impl.ManifestEntry;
import com.instaclustr.esop.impl.StorageLocation;
import com.instaclustr.esop.impl.restore.DownloadTracker;
import com.instaclustr.esop.impl.restore.DownloadTracker.DownloadUnit;
import com.instaclustr.esop.impl.restore.RestorationStrategy;
import com.instaclustr.esop.impl.restore.RestorationUtilities;
import com.instaclustr.esop.impl.restore.RestoreOperationRequest;
import com.instaclustr.esop.impl.restore.Restorer;
import com.instaclustr.esop.impl.restore.strategy.DataSynchronizator.ManifestEntrySSTableClassifier;
import com.instaclustr.esop.topology.CassandraClusterTopology.ClusterTopology;
import com.instaclustr.esop.topology.CassandraClusterTopology.ClusterTopology.NodeTopology;
import com.instaclustr.io.FileUtils;
import com.instaclustr.operations.Operation;
import com.instaclustr.operations.Operation.Error;

import static java.lang.String.format;

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
                restorer.update(operation.request.storageLocation, null);
                logger.info(format("Updated storage location to %s", operation.request.storageLocation));
            }

            // 2. Download the manifest & tokens, chance to error out soon before we actually pull big files if something goes wrong here
            logger.info("Retrieving manifest for snapshot: {}", request.snapshotTag);

            final Manifest manifest = RestorationUtilities.downloadManifest(operation.request, restorer, null, objectMapper);
            manifest.enrichManifestEntries();

            final DataSynchronizator synchronizator = new DataSynchronizator(manifest, request).execute();

            // here we need to categorize into what data dir entries to download will be downloaded
            // categorization will set "localFile" on manifest entry
            // we need to do it in such a way that sstables belonging together will be placed into same dir

            final ManifestEntrySSTableClassifier classifier = new ManifestEntrySSTableClassifier();
            final Map<String, List<ManifestEntry>> classified = classifier.classify(synchronizator.entriesToDownload());
            classifier.map(classified, request);

            Session<DownloadUnit> downloadSession = null;

            try {
                downloadSession = downloadTracker.submit(restorer,
                                                         operation,
                                                         synchronizator.entriesToDownload(),
                                                         operation.request.snapshotTag, operation.request.concurrentConnections);
                downloadSession.waitUntilConsideredFinished();
                downloadTracker.cancelIfNecessary(downloadSession);
            } finally {
                downloadTracker.removeSession(downloadSession);
            }

            synchronizator.deleteUnnecessarySSTableFiles();
            synchronizator.cleanData();

            // K8S will handle copying over tokens.yaml fragment and disabling bootstrap fragment to right directory to be picked up by Cassandra
            // "standalone / vanilla" Cassandra installations has to cover this manually for now.
            // in the future, we might implement automatic configuration of cassandra.yaml for standalone installations
            if (request.updateCassandraYaml) {

                Path fileToAppendTo;
                boolean shouldAppend = true;

                fileToAppendTo = request.cassandraConfigDirectory.resolve("cassandra.yaml");
                if (!Files.exists(fileToAppendTo)) {
                    logger.info(String.format("File %s does not exist, not going to append to it!", fileToAppendTo));
                    shouldAppend = false;
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
            final String topologyFile = format("topology/%s", request.snapshotTag);
            final String topology = restorer.downloadTopology(Paths.get(topologyFile), fileName -> fileName.contains(topologyFile));
            final ClusterTopology clusterTopology = objectMapper.readValue(topology, ClusterTopology.class);
            // nodeId here is propagated by Cassandra operator and it is "hostname"
            // by translating, we get proper node id (uuid) so we fetch the right node in remote bucket
            return clusterTopology.translateToNodeTopology(request.storageLocation.nodeId);
        } catch (final Exception ex) {
            throw new IllegalStateException(format("Unable to resolve node hostId to restore to for request %s", request), ex);
        }
    }

}