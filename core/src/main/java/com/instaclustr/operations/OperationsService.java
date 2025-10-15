package com.instaclustr.operations;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Inject;
import com.instaclustr.threading.Executors;
import com.instaclustr.threading.Executors.ExecutorServiceSupplier;

import static java.util.Collections.unmodifiableList;
import static java.util.stream.Collectors.toList;

public class OperationsService extends AbstractIdleService {

    private final ListeningExecutorService executorService;
    private final Map<Class<? extends OperationRequest>, OperationFactory> operationFactoriesByRequestType;
    private final Map<UUID, Operation<?>> operations;
    private final BiMap<Class<? extends OperationRequest>, String> typeMappings;

    public OperationsService(ListeningExecutorService executorService) {
        operationFactoriesByRequestType = Collections.emptyMap();
        operations = new HashMap<>();
        typeMappings = ImmutableBiMap.copyOf(Collections.emptyMap());
        this.executorService = executorService;
    }

    @Inject
    public OperationsService(final Map<Class<? extends OperationRequest>, OperationFactory> operationFactoriesByRequestType,
                             final @OperationsMap Map<UUID, Operation<?>> operations,
                             final ExecutorServiceSupplier executorServiceSupplier,
                             final Map<String, Class<? extends OperationRequest>> typeMappings) {
        this.operationFactoriesByRequestType = operationFactoriesByRequestType;
        this.operations = operations;
        this.executorService = executorServiceSupplier.get(Integer.parseInt(System.getProperty("instaclustr.commons.operations.executor.size",
                                                                                               Executors.DEFAULT_CONCURRENT_CONNECTIONS.toString())));
        this.typeMappings = ImmutableBiMap.copyOf(typeMappings).inverse();
    }

    @Override
    protected void startUp() throws Exception {
    }

    @Override
    protected void shutDown() throws Exception {
        MoreExecutors.shutdownAndAwaitTermination(executorService, 1, TimeUnit.MINUTES);
    }

    public void submitOperation(final Operation<?> operation) {
        operations.put(operation.id, operation);
        executorService.submit(operation);
    }

    public void closeOperation(final UUID operationId) {
        Optional<Operation<?>> operation = operation(operationId);
        operation.ifPresent(Operation::close);
    }

    public void closeOperation(final Operation<?> operation) {
        operation.close();
    }

    public Operation<?> submitOperationRequest(final OperationRequest request) {
        final OperationFactory operationFactory = operationFactoriesByRequestType.get(request.getClass());

        final Operation<?> operation = operationFactory.createOperation(request);
        operation.type = typeMappings.get(request.getClass());
        operation.request.type = operation.type;

        submitOperation(operation);

        return operation;
    }

    public Map<UUID, Operation<?>> operations() {
        return Collections.unmodifiableMap(operations);
    }

    public Optional<Operation<?>> operation(final UUID id) {
        return Optional.ofNullable(operations.get(id));
    }

    public boolean noneIsRunning() {
        return allRunning().isEmpty();
    }

    public boolean isAnyRunning() {
        return !allRunning().isEmpty();
    }

    public boolean noneRunningOfTypes(final String... types) {
        return Arrays.stream(types).allMatch(type -> allRunningOfType(type).isEmpty());
    }

    public List<UUID> allOfTypeAndState(final String type, final Operation.State... state) {
        return getIdsOfOperations(operation -> {
            if (operation.request == null) {
                return false;
            }

            if (!type.equals(typeMappings.get(operation.request.getClass()))) {
                return false;
            }

            return Arrays.asList(state).contains(operation.state);
        });
    }

    public List<UUID> allRunningOfType(final String type) {
        return getIdsOfOperations(operation -> isRunning(operation.id) && type.equals(typeMappings.get(operation.request.getClass())));
    }

    public List<UUID> allRunning() {
        return getIdsOfOperations(operation -> !operation.state.isTerminalState());
    }

    public boolean isRunning(final UUID id) {
        return getIdsOfOperations(value -> !value.state.isTerminalState()).contains(id);
    }

    public List<Operation<?>> getOperations(final Predicate<Operation<?>> predicate) {
        return unmodifiableList(operations().values().stream().filter(predicate).collect(toList()));
    }

    public List<UUID> getIdsOfOperations(final Predicate<Operation<?>> predicate) {
        final List<UUID> filteredOperations = new ArrayList<>();

        for (final Entry<UUID, Operation<?>> operation : operations().entrySet()) {
            if (predicate.test(operation.getValue())) {
                filteredOperations.add(operation.getKey());
            }
        }

        return filteredOperations;
    }
}
