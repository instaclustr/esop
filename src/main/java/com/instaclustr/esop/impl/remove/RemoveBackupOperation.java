package com.instaclustr.esop.impl.remove;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;
import com.instaclustr.esop.guice.RestorerFactory;
import com.instaclustr.esop.impl.Manifest.AllManifestsReport;
import com.instaclustr.esop.impl.Manifest.ManifestReporter.ManifestReport;
import com.instaclustr.esop.impl.StorageInteractor;
import com.instaclustr.esop.impl.StorageLocation;
import com.instaclustr.esop.topology.CassandraSimpleTopology;
import com.instaclustr.esop.topology.CassandraSimpleTopology.CassandraSimpleTopologyResult;
import com.instaclustr.measure.Time;
import com.instaclustr.operations.Operation;
import jmx.org.apache.cassandra.service.CassandraJMXService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private List<StorageLocation> getStorageLocations(final StorageInteractor restorer) throws Exception {
        if (request.globalRemoval) {
            return restorer.listNodes(request.dcs);
        }
        else if (request.clusterRemoval) {
            return restorer.listNodes(restorer.listDcs());
        }
        else {
            return Collections.singletonList(request.storageLocation);
        }
    }

    @Override
    protected void run0() throws Exception {
        assert restorerFactoryMap != null;
        assert objectMapper != null;

        request.validate(null);

        if (!request.skipNodeCoordinatesResolution) {
            assert cassandraJMXService != null;
            CassandraSimpleTopologyResult simpleTopology = new CassandraSimpleTopology(cassandraJMXService).act();
            request.storageLocation = StorageLocation.update(request.storageLocation,
                                                             simpleTopology.getClusterName(),
                                                             simpleTopology.getDc(),
                                                             simpleTopology.getHostId());

        }

        try (final StorageInteractor interactor = restorerFactoryMap.get(request.storageLocation.storageProvider).createDeletingInteractor(request)) {
            for (final StorageLocation nodeLocation : getStorageLocations(interactor)) {
                System.out.println(nodeLocation.nodePath());
                logger.info("Looking for backups to delete for node {}", nodeLocation.nodePath());
                interactor.setStorageLocation(nodeLocation);
                request.storageLocation = nodeLocation;
                final Optional<AllManifestsReport> reportOptional = getReport(interactor);

                if (!reportOptional.isPresent()) {
                    logger.info("No backups found for {}", nodeLocation.nodePath());
                    continue;
                }

                final AllManifestsReport report = reportOptional.get();
                final List<ManifestReport> allBackupsToDelete = getBackupsToDelete(report);

                if (allBackupsToDelete.isEmpty()) {
                    if (request.backupName != null) {
                        logger.debug("There is not any {} backup to remove for node {}", request.backupName, nodeLocation);
                    } else {
                        logger.debug("There is not any backup to remove for node {}", nodeLocation);
                    }
                    return;
                }

                logger.info("Removing backups for node {}: {}",
                            nodeLocation.nodePath(),
                            allBackupsToDelete.stream().map(mr -> mr.name).collect(Collectors.joining(",")));

                for (final ManifestReport mr : allBackupsToDelete) {
                    interactor.delete(mr, request);
                }


                for (final ManifestReport mr : allBackupsToDelete) {
                    if (request.globalRemoval) {
                        if (!request.dry) {
                            interactor.deleteTopology(mr.name);
                        } else {
                            logger.info("Deletion of topology for {} was executed in dry mode", mr.name);
                        }
                    }
                }
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
        } else if (request.clusterRemoval){
            manifestReports.addAll(allManifestsReport.reports);
        } else if (request.backupName != null) {
            allManifestsReport.get(request.backupName).map(manifestReports::add);
        } else if (request.olderThan.value > 0) {
            Time time = request.olderThan.asMilliseconds();
            final long cut = this.time - time.value;
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
