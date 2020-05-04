package com.instaclustr.cassandra.backup.impl.refresh;

import com.google.common.base.MoreObjects;
import com.instaclustr.operations.OperationRequest;

public class RefreshOperationRequest extends OperationRequest {

    public final String keyspace;
    public final String table;

    public RefreshOperationRequest(final String keyspace, final String table) {
        this.keyspace = keyspace;
        this.table = table;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("keyspace", keyspace)
            .add("table", table)
            .toString();
    }
}
