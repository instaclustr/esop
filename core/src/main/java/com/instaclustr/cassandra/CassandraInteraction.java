package com.instaclustr.cassandra;

public interface CassandraInteraction<T> {

    T act() throws Exception;
}

