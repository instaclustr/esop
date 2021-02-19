package com.instaclustr.esop.impl.remove;

import java.util.Map;
import java.util.Optional;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;
import com.instaclustr.esop.guice.RestorerFactory;
import com.instaclustr.esop.impl.Manifest.AllManifestsReport;
import com.instaclustr.esop.impl.Manifest.ManifestReporter.ManifestReport;
import com.instaclustr.esop.impl.StorageLocation;
import com.instaclustr.esop.impl.restore.Restorer;
import com.instaclustr.esop.topology.CassandraSimpleTopology;
import com.instaclustr.esop.topology.CassandraSimpleTopology.CassandraSimpleTopologyResult;
import com.instaclustr.operations.Operation;
import jmx.org.apache.cassandra.service.CassandraJMXService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RemoveBackupOperation extends Operation<RemoveBackupRequest> {

    private static final Logger logger = LoggerFactory.getLogger(RemoveBackupOperation.class);

    private final Map<String, RestorerFactory> restorerFactoryMap;
    private final ObjectMapper objectMapper;
    private final CassandraJMXService cassandraJMXService;

    @Inject
    public RemoveBackupOperation(@Assisted final RemoveBackupRequest request,
                                 final CassandraJMXService cassandraJMXService,
                                 final Map<String, RestorerFactory> restorerFactoryMap,
                                 final ObjectMapper objectMapper) {
        super(request);
        this.restorerFactoryMap = restorerFactoryMap;
        this.objectMapper = objectMapper;
        this.cassandraJMXService = cassandraJMXService;
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

        try (final Restorer restorer = restorerFactoryMap.get(request.storageLocation.storageProvider).createDeletingRestorer(request)) {
            final Optional<AllManifestsReport> reportOptional = getReport(restorer);

            if (!reportOptional.isPresent()) {
                return;
            }

            final AllManifestsReport report = reportOptional.get();

            final Optional<ManifestReport> backupToDeleteOptional;

            if (request.removeOldest) {
                backupToDeleteOptional = report.getOldest();
            } else {
                backupToDeleteOptional = report.get(request.backupName);
            }

            if (!backupToDeleteOptional.isPresent()) {
                if (request.backupName != null) {
                    this.addError(Error.from(new IllegalStateException(String.format("There is not any %s backup to remove!", request.backupName))));
                } else {
                    this.addError(Error.from(new IllegalStateException("There is not any backup to remove!")));
                }
                return;
            }

            final ManifestReport backupToDelete = backupToDeleteOptional.get();
            request.report = backupToDelete;

            restorer.delete(backupToDelete, request);
        } catch (final Exception ex) {
            logger.error("Unable to perform backup deletion! - " + ex.getMessage(), ex);
            this.addError(Error.from(ex));
        }
    }

    private Optional<AllManifestsReport> getReport(final Restorer restorer) {
        try {
            return Optional.of(AllManifestsReport.report(restorer.list()));
        } catch (final Exception ex) {
            logger.error("Unable to perform listing! - " + ex.getMessage(), ex);
            this.addError(Error.from(ex));
        }

        return Optional.empty();
    }
}
