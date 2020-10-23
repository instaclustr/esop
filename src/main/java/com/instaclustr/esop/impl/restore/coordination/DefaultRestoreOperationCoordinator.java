package com.instaclustr.esop.impl.restore.coordination;

import java.util.Map;

import com.google.inject.Inject;
import com.instaclustr.esop.guice.RestorerFactory;
import com.instaclustr.esop.impl.restore.RestorationStrategyResolver;
import com.instaclustr.esop.impl.restore.RestoreOperationRequest;
import com.instaclustr.operations.Operation;
import com.instaclustr.operations.Operation.Error;

public class DefaultRestoreOperationCoordinator extends BaseRestoreOperationCoordinator {

    @Inject
    public DefaultRestoreOperationCoordinator(final Map<String, RestorerFactory> restorerFactoryMap,
                                              final RestorationStrategyResolver restorationStrategyResolver) {
        super(restorerFactoryMap, restorationStrategyResolver);
    }

    @Override
    public void coordinate(final Operation<RestoreOperationRequest> operation) throws OperationCoordinatorException {
        if (operation.request.globalRequest) {
            operation.addError(Error.from(new OperationCoordinatorException("This coordinator can not handle global operations.")));
        }

        super.coordinate(operation);
    }
}
