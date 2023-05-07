package com.instaclustr.esop.impl.restore;

import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.instaclustr.esop.impl.restore.RestorationStrategy.RestorationStrategyType;
import com.instaclustr.operations.FunctionWithEx;
import jmx.org.apache.cassandra.service.CassandraJMXService;
import jmx.org.apache.cassandra.service.cassandra3.StorageServiceMBean;

import static java.lang.String.format;
import static java.util.stream.Collectors.toList;

public class RestorationStrategyResolverImpl implements RestorationStrategyResolver {

    private static final Logger logger = LoggerFactory.getLogger(RestorationStrategyResolver.class);

    private final Set<RestorationStrategy> restorationStrategies;
    private final Provider<CassandraJMXService> cassandraJMXServiceProvider;

    @Inject
    public RestorationStrategyResolverImpl(final Set<RestorationStrategy> restorationStrategies,
                                           final Provider<CassandraJMXService> cassandraJMXServiceProvider) {
        this.restorationStrategies = restorationStrategies;
        this.cassandraJMXServiceProvider = cassandraJMXServiceProvider;
    }

    @Override
    public RestorationStrategy resolve(final RestoreOperationRequest request) throws Exception {

        final RestorationStrategyType strategyType = request.restorationStrategyType == null ? RestorationStrategyType.IN_PLACE : request.restorationStrategyType;

        if (strategyType == RestorationStrategyType.IN_PLACE) {
            try {
                cassandraJMXServiceProvider.get().doWithStorageServiceMBean(new FunctionWithEx<StorageServiceMBean, Object>() {
                    @Override
                    public Object apply(final StorageServiceMBean object) {
                        return object.getSchemaVersion();
                    }
                });

                throw new CassandraNodeAlreadyRunningException(String.format("It seems that a Cassandra node is up! %s can not be use while Cassandra is running.", RestorationStrategyType.IN_PLACE));
            } catch (final Exception ex) {
                if (ex instanceof CassandraNodeAlreadyRunningException) {
                    throw new IllegalStateException(ex);
                }
                // catching exception here is otherwise what we want
            }
        }

        for (final RestorationStrategy strategy : restorationStrategies) {
            if (strategy.getStrategyType() == strategyType) {
                logger.info("Resolved {}", strategy.getClass().getName());
                return strategy;
            }
        }

        throw new IllegalStateException(format("There is not a restoration strategy of type %s, only %s",
                                               strategyType.toValue(),
                                               restorationStrategies.stream().map(strategy -> strategy.getStrategyType().toString()).collect(toList())));
    }

    public static final class CassandraNodeAlreadyRunningException extends RuntimeException {

        public CassandraNodeAlreadyRunningException(final String message) {
            super(message);
        }
    }
}
