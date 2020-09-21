package com.instaclustr.cassandra.backup.impl;

import java.util.HashSet;
import java.util.Set;

import com.instaclustr.operations.OperationRequest;
import com.instaclustr.operations.ResultGatherer.ResultEntry;

public final class GatheringOperationCoordinatorException extends RuntimeException {

    private final Set<ResultEntry<?>> results = new HashSet<>();

    public GatheringOperationCoordinatorException(final Set<? extends ResultEntry<? extends OperationRequest>> results) {
        this.results.addAll(results);
    }


    @Override
    public String toString() {
        return results.toString();
    }
}
