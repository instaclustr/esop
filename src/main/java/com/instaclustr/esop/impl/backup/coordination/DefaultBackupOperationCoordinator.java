package com.instaclustr.esop.impl.backup.coordination;

import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.instaclustr.esop.guice.BackuperFactory;
import com.instaclustr.esop.guice.BucketServiceFactory;
import com.instaclustr.esop.impl.backup.BackupOperationRequest;
import com.instaclustr.esop.impl.backup.BackupPhaseResultGatherer;
import com.instaclustr.esop.impl.backup.UploadTracker;
import com.instaclustr.esop.impl.interaction.CassandraSchemaVersion;
import com.instaclustr.operations.Operation;
import com.instaclustr.operations.ResultGatherer;
import jmx.org.apache.cassandra.service.CassandraJMXService;

public class DefaultBackupOperationCoordinator extends BaseBackupOperationCoordinator {

    @Inject
    public DefaultBackupOperationCoordinator(final CassandraJMXService cassandraJMXService,
                                             final Map<String, BackuperFactory> backuperFactoryMap,
                                             final Map<String, BucketServiceFactory> bucketServiceFactoryMap,
                                             final ObjectMapper objectMapper,
                                             final UploadTracker uploadTracker) {
        super(cassandraJMXService,
              backuperFactoryMap,
              bucketServiceFactoryMap,
              objectMapper,
              uploadTracker);
    }

    @Override
    public ResultGatherer<BackupOperationRequest> coordinate(final Operation<BackupOperationRequest> operation) {
        final BackupPhaseResultGatherer gatherer = new BackupPhaseResultGatherer();

        if (operation.request.globalRequest) {
            gatherer.gather(operation, new OperationCoordinatorException("This coordinator can not handle global operations."));
            return gatherer;
        }

        try {
            operation.request.schemaVersion = new CassandraSchemaVersion(cassandraJMXService).act();
            operation.request.snapshotTag = resolveSnapshotTag(operation.request, System.currentTimeMillis());
            return super.coordinate(operation);
        } catch (final Exception ex) {
            gatherer.gather(operation, new OperationCoordinatorException("Error occurred during backup request: " + ex.getMessage(), ex));
            return gatherer;
        }
    }
}
