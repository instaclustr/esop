package com.instaclustr.esop.impl.remove;

import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;
import com.instaclustr.esop.guice.RestorerFactory;
import com.instaclustr.esop.impl.Manifest.AllManifestsReport;
import com.instaclustr.esop.impl.Manifest.ManifestReporter.ManifestReport;
import com.instaclustr.esop.impl.ProxySettings;
import com.instaclustr.esop.impl.StorageInteractor;
import com.instaclustr.esop.impl.StorageLocation;
import com.instaclustr.esop.impl.retry.RetrySpec;
import com.instaclustr.esop.local.LocalFileRestorer;
import com.instaclustr.esop.topology.CassandraSimpleTopology;
import com.instaclustr.esop.topology.CassandraSimpleTopology.CassandraSimpleTopologyResult;
import com.instaclustr.measure.Time;
import com.instaclustr.operations.Operation;
import jmx.org.apache.cassandra.service.CassandraJMXService;

import static com.instaclustr.esop.impl.list.ListOperationRequest.getForLocalListing;

public class RemoveBackupOperation extends Operation<RemoveBackupRequest> {

    private static final Logger logger = LoggerFactory.getLogger(RemoveBackupOperation.class);

    private final Map<String, RestorerFactory> restorerFactoryMap;
    private final ObjectMapper objectMapper;
    private final CassandraJMXService cassandraJMXService;
    private final long time;

    @Inject
    public RemoveBackupOperation(@Assisted final RemoveBackupRequest request,
                                 final CassandraJMXService cassandraJMXService,
                                 final Map<String, RestorerFactory> restorerFactoryMap,
                                 final ObjectMapper objectMapper) {
        super(request);
        time = System.currentTimeMillis();
        this.restorerFactoryMap = restorerFactoryMap;
        this.objectMapper = objectMapper;
        this.cassandraJMXService = cassandraJMXService;
    }

    @JsonCreator
    private RemoveBackupOperation(@JsonProperty("type") final String type,
                                  @JsonProperty("id") final UUID id,
                                  @JsonProperty("creationTime") final Instant creationTime,
                                  @JsonProperty("state") final State state,
                                  @JsonProperty("errors") final List<Error> errors,
                                  @JsonProperty("progress") final float progress,
                                  @JsonProperty("startTime") final Instant startTime,
                                  @JsonProperty("storageLocation") final StorageLocation storageLocation,
                                  @JsonProperty("k8sNamespace") final String k8sNamespace,
                                  @JsonProperty("k8sSecretName") final String k8sSecretName,
                                  @JsonProperty("insecure") final boolean insecure,
                                  @JsonProperty("skipBucketVerification") final boolean skipBucketVerification,
                                  @JsonProperty("proxySettings") final ProxySettings proxySettings,
                                  @JsonProperty("retry") final RetrySpec retry,
                                  @JsonProperty("backupName") final String backupName,
                                  @JsonProperty("dry") final boolean dry,
                                  @JsonProperty("resolveNodes") final boolean resolveNodes,
                                  @JsonProperty("olderThan") final Time olderThan,
                                  @JsonProperty("cacheDir") final Path cacheDir,
                                  @JsonProperty("removeOldest") final boolean removeOldest,
                                  @JsonProperty("concurrentConnections") final Integer concurrentConnections,
                                  @JsonProperty("globalRequest") final boolean globalRequest) {
        super(type, id, creationTime, state, errors, progress, startTime, new RemoveBackupRequest(type,
                                                                                                  storageLocation,
                                                                                                  insecure,
                                                                                                  skipBucketVerification,
                                                                                                  proxySettings,
                                                                                                  retry,
                                                                                                  backupName,
                                                                                                  dry,
                                                                                                  resolveNodes,
                                                                                                  olderThan,
                                                                                                  cacheDir,
                                                                                                  removeOldest,
                                                                                                  concurrentConnections,
                                                                                                  globalRequest));
        this.restorerFactoryMap = null;
        this.objectMapper = null;
        this.cassandraJMXService = null;
        this.time = System.currentTimeMillis();
    }

    private List<StorageLocation> getStorageLocations(final StorageInteractor restorer) throws Exception {
        if (request.globalRequest) {
            return restorer.listNodes(request.dcs);
        } else {
            return Collections.singletonList(request.storageLocation);
        }
    }

    @Override
    protected void run0() throws Exception {
        assert restorerFactoryMap != null;
        assert objectMapper != null;

        request.validate(null);

        if (request.resolveNodes) {
            assert cassandraJMXService != null;
            CassandraSimpleTopologyResult simpleTopology = new CassandraSimpleTopology(cassandraJMXService).act();
            request.storageLocation = StorageLocation.update(request.storageLocation,
                                                             simpleTopology.getClusterName(),
                                                             simpleTopology.getDc(),
                                                             simpleTopology.getHostId());
        }

        try (final StorageInteractor interactor = restorerFactoryMap.get(request.storageLocation.storageProvider).createDeletingInteractor(request)) {
            interactor.update(request.storageLocation, new LocalFileRestorer(getForLocalListing(request,
                                                                                                request.cacheDir,
                                                                                                request.storageLocation),
                                                                             objectMapper));
            for (final StorageLocation nodeLocation : getStorageLocations(interactor)) {
                logger.info("Looking for backups to delete for node {}", nodeLocation.nodePath());
                request.storageLocation = nodeLocation;
                interactor.update(nodeLocation, new LocalFileRestorer(getForLocalListing(request,
                                                                                         request.cacheDir,
                                                                                         request.storageLocation),
                                                                      objectMapper));

                final Optional<AllManifestsReport> reportOptional = getReport(interactor);

                if (!reportOptional.isPresent()) {
                    logger.info("No backups found for {}", nodeLocation.nodePath());
                    continue;
                }

                final AllManifestsReport report = reportOptional.get();
                logger.debug(report.toString());
                final List<ManifestReport> allBackupsToDelete = getBackupsToDelete(report);

                if (allBackupsToDelete.isEmpty()) {
                    if (request.backupName != null) {
                        logger.info("There is not any {} backup to remove for node {}", request.backupName, nodeLocation);
                    } else {
                        logger.info("There is not any backup to remove for node {}", nodeLocation);
                    }
                    continue;
                }

                logger.info("Removing backups for node {}: {}",
                            nodeLocation.nodePath(),
                            allBackupsToDelete.stream().map(mr -> mr.name).collect(Collectors.joining(",")));

                for (final ManifestReport mr : allBackupsToDelete) {
                    interactor.delete(mr, request);
                }
            }

            if (!request.dry) {
                interactor.deleteTopology(request.backupName);
            } else {
                logger.info("Deletion of topology for {} was executed in dry mode", request.backupName);
            }
        } catch (final Exception ex) {
            logger.error("Unable to perform backup deletion! - " + ex.getMessage(), ex);
            this.addError(Error.from(ex));
        }
    }

    private List<ManifestReport> getBackupsToDelete(final AllManifestsReport allManifestsReport) {
        final List<ManifestReport> manifestReports = new ArrayList<>();

        if (request.removeOldest) {
            allManifestsReport.getOldest().map(manifestReports::add);
        } else if (request.backupName != null) {
            allManifestsReport.get(request.backupName).map(manifestReports::add);
        } else if (request.olderThan.value > 0) {
            final long cut = this.time - request.olderThan.toMilliseconds();
            manifestReports.addAll(allManifestsReport.filter(report -> report.unixtimestamp < cut));
        }

        return manifestReports;
    }

    private Optional<AllManifestsReport> getReport(final StorageInteractor storageInteractor) {
        try {
            return Optional.of(AllManifestsReport.report(storageInteractor.listManifests()));
        } catch (final Exception ex) {
            logger.error(String.format("Unable to perform listing against node %s - %s", storageInteractor.getStorageLocation(), ex.getMessage()), ex);
            this.addError(Error.from(ex));
        }

        return Optional.empty();
    }
}
