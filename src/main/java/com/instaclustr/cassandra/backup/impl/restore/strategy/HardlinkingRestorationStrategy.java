package com.instaclustr.cassandra.backup.impl.restore.strategy;

import static com.instaclustr.cassandra.backup.impl.restore.RestorationPhase.RestorationPhaseType.CLEANUP;
import static com.instaclustr.cassandra.backup.impl.restore.RestorationPhase.RestorationPhaseType.DOWNLOAD;
import static com.instaclustr.cassandra.backup.impl.restore.RestorationPhase.RestorationPhaseType.IMPORT;
import static com.instaclustr.cassandra.backup.impl.restore.RestorationPhase.RestorationPhaseType.INIT;
import static com.instaclustr.cassandra.backup.impl.restore.RestorationPhase.RestorationPhaseType.TRUNCATE;
import static com.instaclustr.cassandra.backup.impl.restore.RestorationStrategy.RestorationStrategyType.HARDLINKS;
import static java.lang.String.format;

import com.google.inject.Inject;
import com.instaclustr.cassandra.backup.impl.restore.RestorationPhase;
import com.instaclustr.cassandra.backup.impl.restore.RestorationPhase.CleaningPhase;
import com.instaclustr.cassandra.backup.impl.restore.RestorationPhase.DownloadingPhase;
import com.instaclustr.cassandra.backup.impl.restore.RestorationPhase.HardlinkingPhase;
import com.instaclustr.cassandra.backup.impl.restore.RestorationPhase.InitPhase;
import com.instaclustr.cassandra.backup.impl.restore.RestorationPhase.RestorationPhaseType;
import com.instaclustr.cassandra.backup.impl.restore.RestorationPhase.TruncatingPhase;
import com.instaclustr.cassandra.backup.impl.restore.RestoreOperationRequest;
import com.instaclustr.cassandra.backup.impl.restore.Restorer;
import com.instaclustr.operations.Operation;
import jmx.org.apache.cassandra.service.CassandraJMXService;

/**
 * This strategy is supposed to be executed against a running node. This strategy can
 * be executed both for Cassandra 3 and 4 however for 4, {@link ImportingRestorationStrategy} is preferred.
 *
 * <pre>
 * {@code
 * 1) restore the data in a temporary folder
 * 2) truncate all the tables that gonna be restored
 * 3) create hardlinks to the restored files in the target folders
 * 4) do a nodetool refresh (jmx call)
 * 5) clean temporary folder
 * }
 * </pre>
 */
public class HardlinkingRestorationStrategy extends AbstractRestorationStrategy {

    @Inject
    public HardlinkingRestorationStrategy(final CassandraJMXService cassandraJMXService) {
        super(cassandraJMXService);
    }

    @Override
    public RestorationStrategyType getStrategyType() {
        return HARDLINKS;
    }

    @Override
    public RestorationPhase resolveRestorationPhase(final Operation<RestoreOperationRequest> operation, final Restorer restorer) {
        final RestorationPhaseType phaseType = operation.request.restorationPhase;

        if (phaseType == INIT) {
            return new InitPhase(operation);
        } else if (phaseType == DOWNLOAD) {
            return new DownloadingPhase(cassandraJMXService, operation, restorer);
        } else if (phaseType == TRUNCATE) {
            return new TruncatingPhase(cassandraJMXService, operation);
        } else if (phaseType == IMPORT) {
            return new HardlinkingPhase(cassandraJMXService, operation);
        } else if (phaseType == CLEANUP) {
            return new CleaningPhase(operation);
        }

        throw new IllegalStateException(format("Unable to resolve phase for phase type %s for %s.", phaseType, HardlinkingRestorationStrategy.class.getName()));
    }
}
