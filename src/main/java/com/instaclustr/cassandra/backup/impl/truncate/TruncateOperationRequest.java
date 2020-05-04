package com.instaclustr.cassandra.backup.impl.truncate;

import javax.validation.constraints.NotEmpty;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import com.instaclustr.operations.OperationRequest;

public class TruncateOperationRequest extends OperationRequest {

    @NotEmpty
    public String keyspace;

    @NotEmpty
    public String table;

    public TruncateOperationRequest(@NotEmpty final String keyspace, @NotEmpty final String table) {
        this("truncate", keyspace, table);
    }

    @JsonCreator
    public TruncateOperationRequest(@JsonProperty("type") final String type,
                                    @JsonProperty("keyspace") final String keyspace,
                                    @JsonProperty("table") final String table) {
        this.keyspace = keyspace;
        this.table = table;
        this.type = type;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("keyspace", keyspace)
            .add("table", table)
            .toString();
    }
}
