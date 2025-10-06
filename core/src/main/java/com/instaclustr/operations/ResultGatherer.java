package com.instaclustr.operations;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.Sets;
import com.instaclustr.operations.Operation.State;

public abstract class ResultGatherer<T extends OperationRequest> {

    private final Collection<ResultEntry<T>> resultEntries = new ArrayList<>();

    public synchronized ResultGatherer<T> combine(final ResultGatherer<T> anotherGatherer) {
        this.resultEntries.addAll(anotherGatherer.resultEntries);
        return this;
    }

    public synchronized ResultGatherer<T> gather(final Operation<T> operation, final Throwable stageThrowable) {
        resultEntries.add(new ResultEntry<>(operation, stageThrowable));
        return this;
    }

    public Collection<ResultEntry<T>> getFinishedOperations() {
        return Collections.unmodifiableCollection(resultEntries);
    }

    public Set<ResultEntry<T>> getExceptionallyFinishedOperations() {
        return resultEntries.stream().filter(entry -> entry.exceptionMessage != null).collect(Collectors.toSet());
    }

    public Set<ResultEntry<T>> getFailedOperations() {
        return resultEntries.stream().filter(entry -> entry.failed).collect(Collectors.toSet());
    }

    public Set<ResultEntry<T>> getErrorneousOperations() {
        return Sets.union(getFailedOperations(), getExceptionallyFinishedOperations());
    }

    public boolean hasExceptionallyFinishedOperations() {
        return !getExceptionallyFinishedOperations().isEmpty();
    }

    public boolean hasFailedOperations() {
        return !getFailedOperations().isEmpty();
    }

    public boolean hasErrors() {
        return hasFailedOperations() || hasExceptionallyFinishedOperations();
    }

    public static class ResultEntry<T extends OperationRequest> {

        public boolean failed;
        public String operation;
        public String exceptionMessage;

        public ResultEntry(final Operation<T> operation, final Throwable throwable) {
            this.operation = operation.toString();
            this.failed = operation.state == State.FAILED;
            if (throwable != null) {
                this.exceptionMessage = throwable.toString();
            }
        }

        @Override
        public String toString() {
            return "ResultEntry{" +
                "failed=" + failed +
                ", operation='" + operation + '\'' +
                ", exceptionMessage='" + exceptionMessage + '\'' +
                '}';
        }
    }
}
