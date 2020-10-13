package com.instaclustr.esop.impl.backup.coordination;

import javax.inject.Provider;
import java.util.HashMap;

import com.instaclustr.cassandra.CassandraVersion;
import com.instaclustr.esop.impl.DatabaseEntities;
import com.instaclustr.esop.impl.backup.coordination.TakeSnapshotOperation.TakeSnapshotOperationRequest;
import com.instaclustr.operations.FunctionWithEx;
import com.instaclustr.operations.Operation;
import com.instaclustr.operations.OperationRequest;
import jmx.org.apache.cassandra.service.CassandraJMXService;
import jmx.org.apache.cassandra.service.cassandra2.Cassandra2StorageServiceMBean;
import jmx.org.apache.cassandra.service.cassandra3.StorageServiceMBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        if (request.entities.areEmpty()) {
            logger.info("Taking snapshot '{}' on all keyspaces.", request.tag);
        } else {
            logger.info("Taking snapshot '{}' on {}", request.tag, request.entities);
        }

        if (cassandraVersionProvider.get().getMajor() == 2) {
            takeSnapshotForCassandra2();
        } else {
            takeSnapshot();
        }
    }

    private void takeSnapshotForCassandra2() throws Exception {
        cassandraJMXService.doWithCassandra2StorageServiceMBean(new FunctionWithEx<Cassandra2StorageServiceMBean, Void>() {
            @Override
            public Void apply(final Cassandra2StorageServiceMBean object) throws Exception {
                if (request.entities.areEmpty()) {
                    object.takeSnapshot(request.tag);
                } else {
                    if (request.entities.keyspacesOnly()) {
                        object.takeSnapshot(request.tag, request.entities.getKeyspaces().toArray(new String[0]));
                    } else {
                        object.takeMultipleColumnFamilySnapshot(request.tag, DatabaseEntities.forTakingSnapshot(request.entities));
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
                    ssMBean.takeSnapshot(request.tag, new HashMap<>());
                } else {
                    ssMBean.takeSnapshot(request.tag, new HashMap<>(), DatabaseEntities.forTakingSnapshot(request.entities));
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
