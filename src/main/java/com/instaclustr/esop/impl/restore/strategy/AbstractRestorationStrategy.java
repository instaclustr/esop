package com.instaclustr.esop.impl.restore.strategy;

import static java.util.stream.Collectors.toSet;
import static java.util.stream.Stream.of;

import java.nio.channels.FileLock;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Provider;
import com.instaclustr.esop.guice.BucketServiceFactory;
import com.instaclustr.esop.impl.restore.DownloadTracker;
import com.instaclustr.esop.impl.restore.RestorationPhase;
import com.instaclustr.esop.impl.restore.RestorationPhase.ClusterHealthCheckPhase;
import com.instaclustr.esop.impl.restore.RestorationPhase.RestorationPhaseType;
import com.instaclustr.esop.impl.restore.RestorationStrategy;
import com.instaclustr.esop.impl.restore.Restorer;
import com.instaclustr.cassandra.CassandraVersion;
import com.instaclustr.esop.impl.restore.RestoreOperationRequest;
import com.instaclustr.io.GlobalLock;
import com.instaclustr.operations.Operation;
import jmx.org.apache.cassandra.service.CassandraJMXService;

public abstract class AbstractRestorationStrategy implements RestorationStrategy {

    protected final CassandraJMXService cassandraJMXService;
    protected final Provider<CassandraVersion> cassandraVersion;
    protected final ObjectMapper objectMapper;
    protected final DownloadTracker downloadTracker;
    protected final Map<String, BucketServiceFactory> bucketServiceFactoryMap;

    public AbstractRestorationStrategy(final CassandraJMXService cassandraJMXService,
                                       final Provider<CassandraVersion> cassandraVersion,
                                       final ObjectMapper objectMapper,
                                       final DownloadTracker downloadTracker,
                                       final Map<String, BucketServiceFactory> bucketServiceFactoryMap) {
        this.cassandraJMXService = cassandraJMXService;
        this.cassandraVersion = cassandraVersion;
        this.objectMapper = objectMapper;
        this.downloadTracker = downloadTracker;
        this.bucketServiceFactoryMap = bucketServiceFactoryMap;
    }

    public abstract RestorationPhase resolveRestorationPhase(Operation<RestoreOperationRequest> operation, Restorer restorer);

    protected RestorationContext initialiseRestorationContext(final Operation<RestoreOperationRequest> operation,
                                                              final Restorer restorer,
                                                              final ObjectMapper objectMapper,
                                                              final Provider<CassandraVersion> cassandraVersion,
                                                              final DownloadTracker downloadTracker,
                                                              final Map<String, BucketServiceFactory> bucketServiceFactoryMap) {
        final RestorationPhaseType phaseType = operation.request.restorationPhase;

        final RestorationContext ctxt = new RestorationContext();

        ctxt.jmx = cassandraJMXService;
        ctxt.operation = operation;
        ctxt.restorer = restorer;
        ctxt.objectMapper = objectMapper;
        ctxt.phaseType = phaseType;
        ctxt.downloadTracker = downloadTracker;
        ctxt.bucketServiceFactoryMap = bucketServiceFactoryMap;

        if (phaseType == RestorationPhaseType.DOWNLOAD || phaseType == RestorationPhaseType.TRUNCATE || phaseType == RestorationPhaseType.IMPORT) {
            ctxt.cassandraVersion = cassandraVersion.get();
        }

        return ctxt;
    }

    @Override
    public void restore(final Restorer restorer, final Operation<RestoreOperationRequest> operation) throws Exception {

        final FileLock fileLock = new GlobalLock(operation.request.lockFile).waitForLock();

        try {
            final RestorationPhase restorationPhase = resolveRestorationPhase(operation, restorer);

            final Set<RestorationPhaseType> restorationPhaseTypes = of(RestorationPhaseType.DOWNLOAD, RestorationPhaseType.TRUNCATE, RestorationPhaseType.IMPORT).collect(toSet());

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
