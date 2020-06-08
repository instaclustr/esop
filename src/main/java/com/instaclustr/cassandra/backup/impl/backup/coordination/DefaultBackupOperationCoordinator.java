package com.instaclustr.cassandra.backup.impl.backup.coordination;

import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.instaclustr.cassandra.backup.guice.BackuperFactory;
import com.instaclustr.cassandra.backup.guice.BucketServiceFactory;
import com.instaclustr.cassandra.backup.impl.backup.BackupOperationRequest;
import com.instaclustr.cassandra.backup.impl.backup.BackupPhaseResultGatherer;
import com.instaclustr.operations.Operation;
import com.instaclustr.operations.ResultGatherer;
import jmx.org.apache.cassandra.service.CassandraJMXService;

public class DefaultBackupOperationCoordinator extends BaseBackupOperationCoordinator {

    @Inject
    public DefaultBackupOperationCoordinator(final CassandraJMXService cassandraJMXService,
                                             final Map<String, BackuperFactory> backuperFactoryMap,
                                             final Map<String, BucketServiceFactory> bucketServiceFactoryMap,
                                             final ObjectMapper objectMapper) {
        super(cassandraJMXService, backuperFactoryMap, bucketServiceFactoryMap, objectMapper);
    }

    @Override
    public ResultGatherer<BackupOperationRequest> coordinate(final Operation<BackupOperationRequest> operation) throws OperationCoordinatorException {
        if (operation.request.globalRequest) {
            final BackupPhaseResultGatherer gatherer = new BackupPhaseResultGatherer();
            gatherer.gather(operation, new OperationCoordinatorException("This coordinator can not handle global operations."));
            return gatherer;
        }

        return super.coordinate(operation);
    }

}
