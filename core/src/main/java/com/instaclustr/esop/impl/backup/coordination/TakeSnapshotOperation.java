package com.instaclustr.esop.impl.backup.coordination;

import java.util.HashMap;

import com.google.inject.Provider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.instaclustr.cassandra.CassandraVersion;
import com.instaclustr.esop.impl.DatabaseEntities;
import com.instaclustr.esop.impl.backup.coordination.TakeSnapshotOperation.TakeSnapshotOperationRequest;
import com.instaclustr.operations.FunctionWithEx;
import com.instaclustr.operations.Operation;
import com.instaclustr.operations.OperationRequest;
import jmx.org.apache.cassandra.service.CassandraJMXService;
import jmx.org.apache.cassandra.service.cassandra2.Cassandra2StorageServiceMBean;
import jmx.org.apache.cassandra.service.cassandra3.StorageServiceMBean;
import jmx.org.apache.cassandra.service.cassandra30.Cassandra30StorageServiceMBean;

public class TakeSnapshotOperation extends Operation<TakeSnapshotOperationRequest> {

    private static final Logger logger = LoggerFactory.getLogger(TakeSnapshotOperation.class);

    private final TakeSnapshotOperationRequest request;
    private final CassandraJMXService cassandraJMXService;
    private final Provider<CassandraVersion> cassandraVersionProvider;

    public TakeSnapshotOperation(final CassandraJMXService cassandraJMXService,
                                 final TakeSnapshotOperationRequest request,
                                 final Provider<CassandraVersion> cassandraVersionProvider) {
        super(request);
        this.request = request;
        this.cassandraJMXService = cassandraJMXService;
        this.cassandraVersionProvider = cassandraVersionProvider;
    }

    @Override
    protected void run0() throws Exception {
        CassandraVersion cassandraVersion = cassandraVersionProvider.get();
        if (cassandraVersion.getMajor() == 2) {
            takeSnapshotForCassandra2();
        } else if (cassandraVersion.getMajor() == 3 && cassandraVersion.getMinor() == 0) {
            takeSnapshotForCassandra30();
        } else {
            takeSnapshot();
        }
    }

    private void takeSnapshotForCassandra2() throws Exception {
        cassandraJMXService.doWithCassandra2StorageServiceMBean(new FunctionWithEx<Cassandra2StorageServiceMBean, Void>() {
            @Override
            public Void apply(final Cassandra2StorageServiceMBean object) throws Exception {
                if (request.entities.areEmpty()) {
                    logger.info("Taking snapshot '{}' on all keyspaces.", request.tag);
                    object.takeSnapshot(request.tag);
                    logger.info("Snapshot '{}' was taken on all keyspaces.", request.tag);
                } else {
                    if (request.entities.keyspacesOnly()) {
                        logger.info("Taking snapshot '{}' on {}.", request.tag, String.join(",", request.entities.getKeyspaces()));
                        object.takeSnapshot(request.tag, request.entities.getKeyspaces().toArray(new String[0]));
                        logger.info("Snapshot '{}' was taken on {}.", request.tag, String.join(",", request.entities.getKeyspaces()));
                    } else {
                        logger.info("Taking snapshot '{}' on {}.", request.tag, request.entities);
                        object.takeMultipleColumnFamilySnapshot(request.tag, DatabaseEntities.forTakingSnapshot(request.entities));
                        logger.info("Snapshot '{}' was taken on {}.", request.tag, request.entities);
                    }
                }

                return null;
            }
        });
    }

    private void takeSnapshotForCassandra30() throws Exception {
        cassandraJMXService.doWithCassandra30StorageServiceMBean(new FunctionWithEx<Cassandra30StorageServiceMBean, Void>() {
            @Override
            public Void apply(Cassandra30StorageServiceMBean ssMBean) throws Exception {
                if (request.entities.areEmpty()) {
                    logger.info("Taking snapshot '{}' on all keyspaces.", request.tag);
                    ssMBean.takeSnapshot(request.tag);
                    logger.info("Snapshot '{}' was taken on all keyspaces.", request.tag);
                } else {
                    if (request.entities.keyspacesOnly()) {
                        logger.info("Taking snapshot '{}' on {}.", request.tag, String.join(",", request.entities.getKeyspaces()));
                        ssMBean.takeSnapshot(request.tag, request.entities.getKeyspaces().toArray(new String[0]));
                        logger.info("Snapshot '{}' was taken on {}.", request.tag, String.join(",", request.entities.getKeyspaces()));
                    } else {
                        logger.info("Taking snapshot '{}' on {}.", request.tag, request.entities);
                        ssMBean.takeMultipleTableSnapshot(request.tag, DatabaseEntities.forTakingSnapshot(request.entities));
                        logger.info("Snapshot '{}' was taken on {}.", request.tag, request.entities);
                    }
                }

                return null;
            }
        });
    }

    private void takeSnapshot() throws Exception {
        cassandraJMXService.doWithStorageServiceMBean(new FunctionWithEx<StorageServiceMBean, Void>() {
            @Override
            public Void apply(StorageServiceMBean ssMBean) throws Exception {
                if (request.entities.areEmpty()) {
                    logger.info("Taking snapshot '{}' on all keyspaces.", request.tag);
                    ssMBean.takeSnapshot(request.tag, new HashMap<>());
                    logger.info("Snapshot '{}' was taken on all keyspaces.", request.tag);
                } else {
                    logger.info("Taking snapshot '{}' on {}.", request.tag, request.entities);
                    ssMBean.takeSnapshot(request.tag, new HashMap<>(), DatabaseEntities.forTakingSnapshot(request.entities));
                    logger.info("Snapshot '{}' was taken on {}.", request.tag, request.entities);
                }

                return null;
            }
        });
    }

    public static class TakeSnapshotOperationRequest extends OperationRequest {

        final DatabaseEntities entities;
        final String tag;

        public TakeSnapshotOperationRequest(final DatabaseEntities entities, final String tag) {
            this.entities = entities;
            this.tag = tag;
        }
    }
}
