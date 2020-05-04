package com.instaclustr.cassandra.backup.impl.restore;

import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static com.google.inject.util.Types.newParameterizedType;
import static com.instaclustr.operations.OperationBindings.installOperationBindings;

import java.util.Set;

import com.google.inject.AbstractModule;
import com.google.inject.Provider;
import com.google.inject.Provides;
import com.google.inject.TypeLiteral;
import com.google.inject.multibindings.Multibinder;
import com.instaclustr.cassandra.backup.impl.restore.coordination.DefaultRestoreOperationCoordinator;
import com.instaclustr.cassandra.backup.impl.restore.strategy.HardlinkingRestorationStrategy;
import com.instaclustr.cassandra.backup.impl.restore.strategy.ImportingRestorationStrategy;
import com.instaclustr.cassandra.backup.impl.restore.strategy.InPlaceRestorationStrategy;
import com.instaclustr.operations.OperationCoordinator;
import jmx.org.apache.cassandra.service.CassandraJMXService;

public class RestoreModules {

    public static final class RestoreModule extends AbstractModule {

        @Override
        protected void configure() {
            installOperationBindings(binder(),
                                     "restore",
                                     RestoreOperationRequest.class,
                                     RestoreOperation.class);

            @SuppressWarnings("unchecked") final TypeLiteral<OperationCoordinator<RestoreOperationRequest>> operationCoordinator =
                (TypeLiteral<OperationCoordinator<RestoreOperationRequest>>) TypeLiteral.get(newParameterizedType(OperationCoordinator.class, RestoreOperationRequest.class));

            newOptionalBinder(binder(), operationCoordinator).setDefault().to(DefaultRestoreOperationCoordinator.class);
        }
    }

    public static final class RestorationStrategyModule extends AbstractModule {

        @Override
        protected void configure() {
            final Multibinder<RestorationStrategy> restorationStrategyMultibinder = Multibinder.newSetBinder(binder(), RestorationStrategy.class);

            restorationStrategyMultibinder.addBinding().to(InPlaceRestorationStrategy.class);
            restorationStrategyMultibinder.addBinding().to(HardlinkingRestorationStrategy.class);
            restorationStrategyMultibinder.addBinding().to(ImportingRestorationStrategy.class);
        }

        @Provides
        public RestorationStrategyResolver getRestorationStrategyProvider(Set<RestorationStrategy> restorationStrategySet,
                                                                          Provider<CassandraJMXService> cassandraJMXServiceProvider) {
            return new RestorationStrategyResolverImpl(restorationStrategySet, cassandraJMXServiceProvider);
        }
    }

    public static final class RestoreCommitlogModule extends AbstractModule {

        @Override
        protected void configure() {
            installOperationBindings(binder(),
                                     "commitlog-restore",
                                     RestoreCommitLogsOperationRequest.class,
                                     RestoreCommitLogsOperation.class);
        }
    }
}
