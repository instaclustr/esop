package com.instaclustr.esop.cli;

import static com.instaclustr.operations.Operation.State.FAILED;
import static com.instaclustr.picocli.CLIApplication.execute;
import static java.lang.String.format;
import static java.util.Collections.singletonList;
import static org.awaitility.Awaitility.await;

import java.util.List;

import com.google.inject.Inject;
import com.google.inject.Module;
import com.instaclustr.esop.impl.backup.BackupModules.BackupModule;
import com.instaclustr.esop.impl.backup.BackupOperationRequest;
import com.instaclustr.operations.Operation;
import com.instaclustr.operations.OperationsService;
import com.instaclustr.picocli.CassandraJMXSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Spec;

@Command(name = "backup",
    description = "Take a snapshot of a Cassandra node and upload it to remote storage. " +
        "Defaults to a snapshot of all keyspaces and their column families, " +
        "but may be restricted to specific keyspaces or a single column-family.",
    sortOptions = false,
    versionProvider = Esop.class,
    mixinStandardHelpOptions = true
)
public class BackupApplication implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(BackupApplication.class);

    @Spec
    private CommandSpec spec;

    @Mixin
    private CassandraJMXSpec jmxSpec;

    @Mixin
    private BackupOperationRequest request;

    @Inject
    private OperationsService operationsService;

    public static void main(String[] args) {
        System.exit(execute(new BackupApplication(), args));
    }

    @Override
    public void run() {
        Esop.logCommandVersionInformation(spec);

        final List<Module> appSpecificModules = singletonList(new BackupModule());

        Esop.init(this, jmxSpec, request, logger, appSpecificModules);

        final Operation<?> operation = operationsService.submitOperationRequest(request);

        await().forever().until(() -> operation.state.isTerminalState());

        if (operation.state == FAILED) {
            throw new IllegalStateException(format("Backup operation %s was not successful.", operation.id));
        }
    }
}
