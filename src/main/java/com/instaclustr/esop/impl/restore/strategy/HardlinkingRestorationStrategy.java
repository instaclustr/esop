package com.instaclustr.esop.impl.restore.strategy;

import static com.instaclustr.esop.impl.restore.RestorationPhase.RestorationPhaseType.CLEANUP;
import static com.instaclustr.esop.impl.restore.RestorationPhase.RestorationPhaseType.DOWNLOAD;
import static com.instaclustr.esop.impl.restore.RestorationPhase.RestorationPhaseType.IMPORT;
import static com.instaclustr.esop.impl.restore.RestorationPhase.RestorationPhaseType.INIT;
import static com.instaclustr.esop.impl.restore.RestorationPhase.RestorationPhaseType.TRUNCATE;
import static java.lang.String.format;

import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.instaclustr.cassandra.CassandraVersion;
import com.instaclustr.esop.guice.BucketServiceFactory;
import com.instaclustr.esop.impl.hash.HashService;
import com.instaclustr.esop.impl.restore.DownloadTracker;
import com.instaclustr.esop.impl.restore.RestorationPhase;
import com.instaclustr.esop.impl.restore.RestorationPhase.CleaningPhase;
import com.instaclustr.esop.impl.restore.RestorationPhase.DownloadingPhase;
import com.instaclustr.esop.impl.restore.RestorationPhase.HardlinkingPhase;
import com.instaclustr.esop.impl.restore.RestorationPhase.InitPhase;
import com.instaclustr.esop.impl.restore.RestorationPhase.RestorationPhaseType;
import com.instaclustr.esop.impl.restore.RestorationPhase.TruncatingPhase;
import com.instaclustr.esop.impl.restore.RestoreOperationRequest;
import com.instaclustr.esop.impl.restore.Restorer;
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
    public HardlinkingRestorationStrategy(final CassandraJMXService cassandraJMXService,
                                          final Provider<CassandraVersion> cassandraVersion,
                                          final ObjectMapper objectMapper,
                                          final DownloadTracker downloadTracker,
                                          final Map<String, BucketServiceFactory> bucketServiceFactoryMap,
                                          final HashService hashService) {
        super(cassandraJMXService, cassandraVersion, objectMapper, downloadTracker, bucketServiceFactoryMap, hashService);
    }

    @Override
    public RestorationStrategyType getStrategyType() {
        return RestorationStrategyType.HARDLINKS;
    }

    @Override
    public RestorationPhase resolveRestorationPhase(final Operation<RestoreOperationRequest> operation, final Restorer restorer) {

        final RestorationContext ctxt = initialiseRestorationContext(operation,
                                                                     restorer,
                                                                     objectMapper,
                                                                     cassandraVersion,
                                                                     downloadTracker,
                                                                     bucketServiceFactoryMap);

        final RestorationPhaseType phaseType = ctxt.operation.request.restorationPhase;

        try {
            if (phaseType == INIT) {
                return new InitPhase(ctxt);
            } else if (phaseType == DOWNLOAD) {
                return new DownloadingPhase(ctxt);
            } else if (phaseType == TRUNCATE) {
                return new TruncatingPhase(ctxt);
            } else if (phaseType == IMPORT) {
                return new HardlinkingPhase(ctxt);
            } else if (phaseType == CLEANUP) {
                return new CleaningPhase(ctxt);
            }
        } catch (final Exception ex) {
            throw new IllegalStateException(format("Unable to initialise phase %s: ", phaseType.toValue()), ex);
        }

        throw new IllegalStateException(format("Unable to resolve phase for phase type %s for %s.",
                                               phaseType,
                                               HardlinkingRestorationStrategy.class.getName()));
    }
}
