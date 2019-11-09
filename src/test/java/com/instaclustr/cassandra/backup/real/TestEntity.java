package com.instaclustr.cassandra.backup.real;

import java.util.UUID;

import com.datastax.driver.mapping.annotations.ClusteringColumn;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;

@Table(keyspace = TestEntity.KEYSPACE, name = TestEntity.TABLE)
public class TestEntity {

    public static final String KEYSPACE = "test";

    public static final String TABLE = "test";

    public static final String ID = "id";

    public static final String DATE = "date";

    public static final String NAME = "name";

    @PartitionKey
    private String id;

    @ClusteringColumn
    private UUID date;

    @Column
    private String name;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public UUID getDate() {
        return date;
    }

    public void setDate(UUID date) {
        this.date = date;
    }
}
