package com.instaclustr.operations;

public abstract class OperationCoordinator<T extends OperationRequest> {

    public static final int MAX_NUMBER_OF_CONCURRENT_OPERATIONS = Integer.parseInt(System.getProperty("instaclustr.coordinator.operations.executor.size", "100"));

    public abstract void coordinate(final Operation<T> operation) throws OperationCoordinatorException;

    public static class OperationCoordinatorException extends Exception {

        public OperationCoordinatorException(final String message) {
            super(message);
        }

        public OperationCoordinatorException(final String message, final Throwable cause) {
            super(message, cause);
        }
    }
}
