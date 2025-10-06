package com.instaclustr.operations;

public class GlobalOperationProgressTracker {

    private final Operation<? extends OperationRequest> operation;
    private final int operations;

    private float partialUpdate = 0.0f;

    public GlobalOperationProgressTracker(final Operation<? extends OperationRequest> operation,
                                          final int operations) {
        if (operations <= 0) {
            throw new IllegalArgumentException("Events to complete has to be greater than 0.");
        }

        this.operation = operation;
        this.operations = operations;
    }

    public synchronized void update(final float operationProgress) {
        partialUpdate += operationProgress;
        this.operation.progress = partialUpdate / (float) operations;
    }

    public void complete() {
        this.operation.progress = 1.0f;
    }
}
