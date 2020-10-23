package com.instaclustr.esop.cli;

import static com.instaclustr.operations.Operation.State.FAILED;
import static com.instaclustr.picocli.CLIApplication.execute;
import static com.instaclustr.picocli.JarManifestVersionProvider.logCommandVersionInformation;
import static java.lang.String.format;
import static org.awaitility.Awaitility.await;

import java.util.Arrays;

import com.google.inject.Inject;
import com.instaclustr.esop.impl._import.ImportOperationRequest;
import com.instaclustr.esop.impl.restore.RestoreModules.RestorationStrategyModule;
import com.instaclustr.esop.impl.restore.RestoreModules.RestoreModule;
import com.instaclustr.esop.impl.restore.RestoreOperationRequest;
import com.instaclustr.operations.Operation;
import com.instaclustr.operations.OperationsService;
import com.instaclustr.picocli.CassandraJMXSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Spec;

@Command(name = "restore",
    description = "Restore the Cassandra data on this node to a specified point-in-time.",
    sortOptions = false,
    versionProvider = Esop.class,
    mixinStandardHelpOptions = true
)
public class RestoreApplication implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(RestoreApplication.class);

    @Spec
    private CommandSpec spec;

    @Mixin
    private CassandraJMXSpec jmxSpec;

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
        logCommandVersionInformation(spec);

        request.importing = importRequest;

        Esop.init(this, jmxSpec, request, logger, Arrays.asList(new RestoreModule(),
                                                                new RestorationStrategyModule()));

        final Operation<?> operation = operationsService.submitOperationRequest(request);

        await().forever().until(() -> operation.state.isTerminalState());

        if (operation.state == FAILED) {
            throw new IllegalStateException(format("Restore operation %s was not successful.", operation.id));
        }
    }
}
