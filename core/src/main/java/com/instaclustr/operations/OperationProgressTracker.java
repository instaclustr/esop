package com.instaclustr.operations;

public class OperationProgressTracker {

    private final Operation<? extends OperationRequest> operation;
    private final int eventsToComplete;
    private int eventsCompleted = 0;

    private float partialUpdate = 0.0f;

    public OperationProgressTracker(final Operation<? extends OperationRequest> operation,
                                    final int eventsToComplete) {
        if (eventsToComplete <= 0) {
            throw new IllegalArgumentException("Events to complete has to be greater than 0.");
        }

        this.operation = operation;
        this.eventsToComplete = eventsToComplete;
    }

    public synchronized void update() {
        this.operation.progress = (float) ++eventsCompleted / (float) eventsToComplete;
    }

    public void complete() {
        this.operation.progress = 1.0f;
    }
}

