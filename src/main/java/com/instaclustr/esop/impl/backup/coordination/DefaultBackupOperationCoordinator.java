package com.instaclustr.esop.impl.backup.coordination;

import javax.inject.Provider;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.instaclustr.cassandra.CassandraVersion;
import com.instaclustr.esop.guice.BackuperFactory;
import com.instaclustr.esop.guice.BucketServiceFactory;
import com.instaclustr.esop.impl.backup.BackupOperationRequest;
import com.instaclustr.esop.impl.backup.UploadTracker;
import com.instaclustr.esop.impl.hash.HashSpec;
import com.instaclustr.esop.impl.interaction.CassandraSchemaVersion;
import com.instaclustr.operations.Operation;
import com.instaclustr.operations.Operation.Error;
import jmx.org.apache.cassandra.service.CassandraJMXService;

public class DefaultBackupOperationCoordinator extends BaseBackupOperationCoordinator {

    @Inject
    public DefaultBackupOperationCoordinator(final CassandraJMXService cassandraJMXService,
                                             final Provider<CassandraVersion> cassandraVersionProvider,
                                             final Map<String, BackuperFactory> backuperFactoryMap,
                                             final Map<String, BucketServiceFactory> bucketServiceFactoryMap,
                                             final ObjectMapper objectMapper,
                                             final UploadTracker uploadTracker,
                                             final HashSpec hashSpec) {
        super(cassandraJMXService,
              cassandraVersionProvider,
              backuperFactoryMap,
              bucketServiceFactoryMap,
              objectMapper,
              uploadTracker,
              hashSpec);
    }

    @Override
    public void coordinate(final Operation<BackupOperationRequest> operation) {
        if (operation.request.globalRequest) {
            operation.addError(Error.from(new OperationCoordinatorException("This coordinator can not handle global operations.")));
        }

        try {
            operation.request.schemaVersion = new CassandraSchemaVersion(cassandraJMXService).act();
            operation.request.snapshotTag = resolveSnapshotTag(operation.request, System.currentTimeMillis());
            super.coordinate(operation);
        } catch (final Exception ex) {
            operation.addError(Error.from(new OperationCoordinatorException("Error occurred during backup request: " + ex.getMessage(), ex)));
        }
    }
}
