package com.instaclustr.operations;

@FunctionalInterface
public interface OperationCoordinatorFactory<RequestT extends OperationRequest> {

    OperationCoordinator createOperationCoordinator();
}
