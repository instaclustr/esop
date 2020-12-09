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
import com.instaclustr.esop.impl.restore.DownloadTracker;
import com.instaclustr.esop.impl.restore.RestorationPhase;
import com.instaclustr.esop.impl.restore.RestorationPhase.CleaningPhase;
import com.instaclustr.esop.impl.restore.RestorationPhase.DownloadingPhase;
import com.instaclustr.esop.impl.restore.RestorationPhase.ImportingPhase;
import com.instaclustr.esop.impl.restore.RestorationPhase.InitPhase;
import com.instaclustr.esop.impl.restore.RestorationPhase.RestorationPhaseType;
import com.instaclustr.esop.impl.restore.RestorationPhase.TruncatingPhase;
import com.instaclustr.esop.impl.restore.RestoreOperationRequest;
import com.instaclustr.esop.impl.restore.Restorer;
import com.instaclustr.operations.Operation;
import jmx.org.apache.cassandra.service.CassandraJMXService;

/**
 * This strategy is supposed to be executed against a running Cassandra 4 node. If you run against Cassandra 3,
 * {@link HardlinkingRestorationStrategy} is preferred.
 *
 * <pre>
 * {@code
 * 1) restore the data in a temporary folder
 * 2) truncate data
 * 3) invoke nodetool import command via JMX (only present in Cassandra 4)
 * 4) clean temporary folder
 * }
 * </pre>
 */
public class ImportingRestorationStrategy extends AbstractRestorationStrategy {

    @Inject
    public ImportingRestorationStrategy(final CassandraJMXService cassandraJMXService,
                                        final Provider<CassandraVersion> cassandraVersion,
                                        final ObjectMapper objectMapper,
                                        final DownloadTracker downloadTracker,
                                        final Map<String, BucketServiceFactory> bucketServiceFactoryMap) {
        super(cassandraJMXService, cassandraVersion, objectMapper, downloadTracker, bucketServiceFactoryMap);
    }

    @Override
    public void isEligibleToRun() {
        final CassandraVersion cassandraVersion = this.cassandraVersion.get();

        if (!CassandraVersion.isFour(this.cassandraVersion.get())) {
            throw new IllegalStateException("This type of restoration strategy can be used only against Cassandra 4. " +
                                                "You are running this restoration against " + cassandraVersion.toString());
        }
    }

    @Override
    public RestorationStrategyType getStrategyType() {
        return RestorationStrategyType.IMPORT;
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
                return new ImportingPhase(ctxt);
            } else if (phaseType == CLEANUP) {
                return new CleaningPhase(ctxt);
            }
        } catch (final Exception ex) {
            throw new IllegalStateException(format("Unable to initialise phase %s", phaseType.toValue()));
        }

        throw new IllegalStateException(format("Unable to resolve phase for phase type %s for %s.",
                                               phaseType,
                                               ImportingRestorationStrategy.class.getName()));
    }
}
