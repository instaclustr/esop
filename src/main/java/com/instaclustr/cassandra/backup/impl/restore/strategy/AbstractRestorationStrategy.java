package com.instaclustr.cassandra.backup.impl.restore.strategy;

import static com.instaclustr.cassandra.backup.impl.restore.RestorationPhase.RestorationPhaseType.DOWNLOAD;
import static com.instaclustr.cassandra.backup.impl.restore.RestorationPhase.RestorationPhaseType.IMPORT;
import static com.instaclustr.cassandra.backup.impl.restore.RestorationPhase.RestorationPhaseType.TRUNCATE;
import static java.util.stream.Collectors.toSet;
import static java.util.stream.Stream.of;

import java.nio.channels.FileLock;
import java.util.Set;

import com.instaclustr.cassandra.backup.impl.restore.RestorationPhase;
import com.instaclustr.cassandra.backup.impl.restore.RestorationPhase.ClusterHealthCheckPhase;
import com.instaclustr.cassandra.backup.impl.restore.RestorationPhase.RestorationPhaseType;
import com.instaclustr.cassandra.backup.impl.restore.RestorationStrategy;
import com.instaclustr.cassandra.backup.impl.restore.RestoreOperationRequest;
import com.instaclustr.cassandra.backup.impl.restore.Restorer;
import com.instaclustr.io.GlobalLock;
import com.instaclustr.operations.Operation;
import jmx.org.apache.cassandra.service.CassandraJMXService;

public abstract class AbstractRestorationStrategy implements RestorationStrategy {

    protected final CassandraJMXService cassandraJMXService;

    public AbstractRestorationStrategy(final CassandraJMXService cassandraJMXService) {
        this.cassandraJMXService = cassandraJMXService;
    }

    public abstract RestorationPhase resolveRestorationPhase(Operation<RestoreOperationRequest> operation, Restorer restorer);

    @Override
    public void restore(final Restorer restorer, final Operation<RestoreOperationRequest> operation) throws Exception {

        final FileLock fileLock = new GlobalLock(operation.request.lockFile).waitForLock();

        try {
            final RestorationPhase restorationPhase = resolveRestorationPhase(operation, restorer);

            final Set<RestorationPhaseType> restorationPhaseTypes = of(DOWNLOAD, TRUNCATE, IMPORT).collect(toSet());

            if (restorationPhaseTypes.contains(restorationPhase.getRestorationPhaseType())) {
                new ClusterHealthCheckPhase(cassandraJMXService, operation).execute();
            }

            restorationPhase.execute();
        } finally {
            fileLock.release();
        }
    }
}
