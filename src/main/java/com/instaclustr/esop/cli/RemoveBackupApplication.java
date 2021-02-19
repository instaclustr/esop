package com.instaclustr.esop.cli;

import static com.instaclustr.operations.Operation.State.FAILED;
import static com.instaclustr.picocli.CLIApplication.execute;
import static java.lang.String.format;
import static org.awaitility.Awaitility.await;

import java.util.Collections;
import java.util.List;

import com.google.inject.Inject;
import com.google.inject.Module;
import com.instaclustr.esop.impl.hash.HashSpec;
import com.instaclustr.esop.impl.remove.RemoveBackupModule;
import com.instaclustr.esop.impl.remove.RemoveBackupRequest;
import com.instaclustr.operations.Operation;
import com.instaclustr.operations.OperationsService;
import com.instaclustr.picocli.CassandraJMXSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Spec;

@Command(name = "remove-backup",
    description = "remove a backup from remote location",
    sortOptions = false,
    versionProvider = Esop.class,
    mixinStandardHelpOptions = true
)
public class RemoveBackupApplication implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(RemoveBackupApplication.class);

    @Spec
    private CommandSpec spec;

    @Mixin
    private RemoveBackupRequest request;

    @Mixin
    private CassandraJMXSpec jmxSpec;

    @Inject
    private OperationsService operationsService;

    public static void main(String[] args) {
        System.exit(execute(new ListApplication(), args));
    }

    @Override
    public void run() {
        Esop.logCommandVersionInformation(spec);

        final List<Module> modules = Collections.singletonList(new RemoveBackupModule());

        Esop.init(this, jmxSpec, new HashSpec(), request, logger, modules);

        final Operation<?> operation = operationsService.submitOperationRequest(request);

        await().forever().until(() -> operation.state.isTerminalState());

        if (operation.state == FAILED) {
            throw new IllegalStateException(format("List operation %s was not successful.", operation.id));
        }
    }
}

