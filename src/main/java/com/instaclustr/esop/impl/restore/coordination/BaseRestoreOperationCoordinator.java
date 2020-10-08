package com.instaclustr.esop.impl.restore.coordination;

import static java.lang.String.format;

import java.util.Map;

import com.instaclustr.esop.guice.RestorerFactory;
import com.instaclustr.esop.impl.restore.RestorationPhaseResultGatherer;
import com.instaclustr.esop.impl.restore.RestorationStrategy;
import com.instaclustr.esop.impl.restore.RestorationStrategy.RestorationStrategyType;
import com.instaclustr.esop.impl.restore.RestorationStrategyResolver;
import com.instaclustr.esop.impl.restore.Restorer;
import com.instaclustr.esop.impl.restore.RestoreOperationRequest;
import com.instaclustr.operations.Operation;
import com.instaclustr.operations.OperationCoordinator;
import com.instaclustr.operations.ResultGatherer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BaseRestoreOperationCoordinator extends OperationCoordinator<RestoreOperationRequest> {

    private static final Logger logger = LoggerFactory.getLogger(BaseRestoreOperationCoordinator.class);

    private final Map<String, RestorerFactory> restorerFactoryMap;
    private final RestorationStrategyResolver restorationStrategyResolver;

    public BaseRestoreOperationCoordinator(final Map<String, RestorerFactory> restorerFactoryMap,
                                           final RestorationStrategyResolver restorationStrategyResolver) {
        this.restorerFactoryMap = restorerFactoryMap;
        this.restorationStrategyResolver = restorationStrategyResolver;
    }

    @Override
    public ResultGatherer<RestoreOperationRequest> coordinate(final Operation<RestoreOperationRequest> operation) throws OperationCoordinatorException {

        final RestoreOperationRequest request = operation.request;

        if (request.restorationStrategyType == RestorationStrategyType.IMPORT || request.restorationStrategyType == RestorationStrategyType.HARDLINKS) {
            if (request.importing == null) {
                throw new IllegalStateException(format("you can not run %s strategy and have 'import' empty!",
                                                       request.restorationStrategyType));
            }

            if (request.restorationPhase == null) {
                throw new IllegalStateException(format("you can not run %s strategy and have 'restorationPhase' empty!",
                                                       request.restorationStrategyType));
            }
        }

        if (request.restoreSystemKeyspace && (request.restorationStrategyType != RestorationStrategyType.IN_PLACE)) {
            throw new IllegalStateException("you can not set 'restoreSystemKeyspace' to true when your restoration strategy is not IN_PLACE, "
                                                + "it is not possible to restore system keyspace on a running node");
        }

        final RestorationPhaseResultGatherer gatherer = new RestorationPhaseResultGatherer();

        Throwable cause = null;

        try (final Restorer restorer = restorerFactoryMap.get(request.storageLocation.storageProvider).createRestorer(request)) {

            final RestorationStrategy restorationStrategy = restorationStrategyResolver.resolve(request);

            restorationStrategy.restore(restorer, operation);

            gatherer.gather(operation, null);
        } catch (final Exception ex) {
            logger.error("Unable to perform restore! - " + ex.getMessage(), ex);
            cause = ex;
        }

        return gatherer.gather(operation, cause);
    }
}
