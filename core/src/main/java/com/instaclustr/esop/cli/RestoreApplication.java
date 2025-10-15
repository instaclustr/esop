package com.instaclustr.esop.cli;

import java.util.ArrayList;
import java.util.List;

import com.google.inject.Inject;
import com.google.inject.Module;
import com.instaclustr.esop.impl._import.ImportOperationRequest;
import com.instaclustr.esop.impl.hash.HashSpec;
import com.instaclustr.esop.impl.restore.RestoreModules.RestorationStrategyModule;
import com.instaclustr.esop.impl.restore.RestoreModules.RestoreModule;
import com.instaclustr.esop.impl.restore.RestoreOperationRequest;
import com.instaclustr.operations.Operation;
import com.instaclustr.operations.OperationsService;
import com.instaclustr.picocli.CassandraJMXSpec;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Spec;

import static com.instaclustr.operations.Operation.State.FAILED;
import static com.instaclustr.picocli.CLIApplication.execute;
import static java.lang.String.format;
import static org.awaitility.Awaitility.await;

@Command(name = "restore",
    description = "Restore the Cassandra data on this node to a specified point-in-time.",
    sortOptions = false,
    versionProvider = Esop.class,
    mixinStandardHelpOptions = true
)
public class RestoreApplication implements Runnable {

    @Spec
    private CommandSpec spec;

    @Mixin
    private CassandraJMXSpec jmxSpec;

    @Mixin
    private HashSpec hashSpec;

    @Mixin
    private RestoreOperationRequest request;

    @Mixin
    private ImportOperationRequest importRequest;

    @Inject
    private OperationsService operationsService;

    public static void main(String[] args) {
        System.exit(execute(new RestoreApplication(), args));
    }

    @Override
    public void run() {
        request.importing = importRequest;

        List<Module> additionalModules = new ArrayList<>(Esop.getStorageSpecificModules());
        additionalModules.add(new RestoreModule());
        additionalModules.add(new RestorationStrategyModule());

        Esop.init(this, jmxSpec, hashSpec, additionalModules);

        final Operation<?> operation = operationsService.submitOperationRequest(request);

        await().forever().until(() -> operation.state.isTerminalState());

        if (operation.state == FAILED) {
            throw new IllegalStateException(format("Restore operation %s was not successful.", operation.id));
        }
    }
}
