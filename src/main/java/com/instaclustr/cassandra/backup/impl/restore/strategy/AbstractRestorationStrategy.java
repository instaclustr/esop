package com.instaclustr.cassandra.backup.impl.restore.strategy;

import static com.instaclustr.cassandra.backup.impl.restore.RestorationPhase.RestorationPhaseType.DOWNLOAD;
import static com.instaclustr.cassandra.backup.impl.restore.RestorationPhase.RestorationPhaseType.IMPORT;
import static com.instaclustr.cassandra.backup.impl.restore.RestorationPhase.RestorationPhaseType.TRUNCATE;
import static java.util.stream.Collectors.toSet;
import static java.util.stream.Stream.of;

import java.nio.channels.FileLock;
import java.util.Set;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Provider;
import com.instaclustr.cassandra.CassandraVersion;
import com.instaclustr.cassandra.backup.impl.restore.DownloadTracker;
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
    protected final Provider<CassandraVersion> cassandraVersion;
    protected final ObjectMapper objectMapper;
    protected final DownloadTracker downloadTracker;

    public AbstractRestorationStrategy(final CassandraJMXService cassandraJMXService,
                                       final Provider<CassandraVersion> cassandraVersion,
                                       final ObjectMapper objectMapper,
                                       final DownloadTracker downloadTracker) {
        this.cassandraJMXService = cassandraJMXService;
        this.cassandraVersion = cassandraVersion;
        this.objectMapper = objectMapper;
        this.downloadTracker = downloadTracker;
    }

    public abstract RestorationPhase resolveRestorationPhase(Operation<RestoreOperationRequest> operation, Restorer restorer);

    public RestorationContext initialiseRestorationContext(final Operation<RestoreOperationRequest> operation,
                                                           final Restorer restorer,
                                                           final ObjectMapper objectMapper,
                                                           final Provider<CassandraVersion> cassandraVersion,
                                                           final DownloadTracker downloadTracker) {
        final RestorationPhaseType phaseType = operation.request.restorationPhase;

        final RestorationContext ctxt = new RestorationContext();

        ctxt.jmx = cassandraJMXService;
        ctxt.operation = operation;
        ctxt.restorer = restorer;
        ctxt.objectMapper = objectMapper;
        ctxt.phaseType = phaseType;
        ctxt.downloadTracker = downloadTracker;

        if (phaseType == DOWNLOAD || phaseType == TRUNCATE || phaseType == RestorationPhaseType.IMPORT) {
            ctxt.cassandraVersion = cassandraVersion.get();
        }

        return ctxt;
    }

    @Override
    public void restore(final Restorer restorer, final Operation<RestoreOperationRequest> operation) throws Exception {

        final FileLock fileLock = new GlobalLock(operation.request.lockFile).waitForLock();

        try {
            final RestorationPhase restorationPhase = resolveRestorationPhase(operation, restorer);

            final Set<RestorationPhaseType> restorationPhaseTypes = of(DOWNLOAD, TRUNCATE, IMPORT).collect(toSet());

            if (restorationPhaseTypes.contains(restorationPhase.getRestorationPhaseType())) {
                final RestorationContext ctxt = new RestorationContext();
                ctxt.jmx = cassandraJMXService;
                ctxt.operation = operation;
                new ClusterHealthCheckPhase(ctxt).execute();
            }

            restorationPhase.execute();
        } finally {
            fileLock.release();
        }
    }
}
