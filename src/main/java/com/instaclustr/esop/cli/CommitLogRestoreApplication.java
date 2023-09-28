package com.instaclustr.esop.cli;

import com.google.inject.Inject;
import com.instaclustr.esop.impl.hash.HashSpec;
import com.instaclustr.esop.impl.restore.RestoreCommitLogsOperationRequest;
import com.instaclustr.esop.impl.restore.RestoreModules.RestoreCommitlogModule;
import com.instaclustr.operations.Operation;
import com.instaclustr.operations.OperationsService;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Spec;

import static com.instaclustr.operations.Operation.State.FAILED;
import static com.instaclustr.picocli.CLIApplication.execute;
import static java.lang.String.format;
import static java.util.Collections.singletonList;
import static org.awaitility.Awaitility.await;

@Command(name = "commitlog-restore",
    description = "Restores archived commit logs to node.",
    sortOptions = false,
    versionProvider = Esop.class,
    mixinStandardHelpOptions = true
)
public class CommitLogRestoreApplication implements Runnable {

    @Spec
    private CommandSpec spec;

    @Mixin
    private HashSpec hashSpec;

    @Mixin
    private RestoreCommitLogsOperationRequest request;

    @Inject
    private OperationsService operationsService;

    public static void main(String[] args) {
        System.exit(execute(new CommitLogRestoreApplication(), args));
    }

    @Override
    public void run() {
        Esop.init(this, null, hashSpec, singletonList(new RestoreCommitlogModule()));

        final Operation<?> operation = operationsService.submitOperationRequest(request);

        await().forever().until(() -> operation.state.isTerminalState());

        if (operation.state == FAILED) {
            throw new IllegalStateException(format("Commitog restore operation %s was not successful.", operation.id));
        }
    }
}
