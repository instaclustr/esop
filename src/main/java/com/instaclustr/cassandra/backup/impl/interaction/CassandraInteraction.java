package com.instaclustr.cassandra.backup.impl.interaction;

public interface CassandraInteraction<T> {

    T act() throws Exception;
}
