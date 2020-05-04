package com.instaclustr.cassandra.backup.impl.restore.coordination;

import java.util.Map;

import com.google.inject.Inject;
import com.instaclustr.cassandra.backup.guice.RestorerFactory;
import com.instaclustr.cassandra.backup.impl.restore.RestorationPhaseResultGatherer;
import com.instaclustr.cassandra.backup.impl.restore.RestorationStrategyResolver;
import com.instaclustr.cassandra.backup.impl.restore.RestoreOperationRequest;
import com.instaclustr.operations.Operation;
import com.instaclustr.operations.ResultGatherer;

public class DefaultRestoreOperationCoordinator extends BaseRestoreOperationCoordinator {

    @Inject
    public DefaultRestoreOperationCoordinator(final Map<String, RestorerFactory> restorerFactoryMap,
                                              final RestorationStrategyResolver restorationStrategyResolver) {
        super(restorerFactoryMap, restorationStrategyResolver);
    }

    @Override
    public ResultGatherer<RestoreOperationRequest> coordinate(final Operation<RestoreOperationRequest> operation) throws OperationCoordinatorException {
        if (operation.request.globalRequest) {
            final RestorationPhaseResultGatherer gatherer = new RestorationPhaseResultGatherer();
            gatherer.gather(operation, new OperationCoordinatorException("This coordinator can not handle global operations."));
            return gatherer;
        }

        return super.coordinate(operation);
    }
}
