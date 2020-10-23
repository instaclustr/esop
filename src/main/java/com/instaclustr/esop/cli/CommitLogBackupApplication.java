package com.instaclustr.esop.cli;

import static com.instaclustr.operations.Operation.State.FAILED;
import static com.instaclustr.picocli.CLIApplication.execute;
import static com.instaclustr.picocli.JarManifestVersionProvider.logCommandVersionInformation;
import static java.lang.String.format;
import static java.util.Collections.singletonList;
import static org.awaitility.Awaitility.await;

import com.google.inject.Inject;
import com.instaclustr.esop.impl.backup.BackupCommitLogsOperationRequest;
import com.instaclustr.esop.impl.backup.BackupModules.CommitlogBackupModule;
import com.instaclustr.operations.Operation;
import com.instaclustr.operations.OperationsService;
import com.instaclustr.picocli.CassandraJMXSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Spec;

@Command(name = "commitlog-backup",
    description = "Upload archived commit logs to remote storage.",
    sortOptions = false,
    versionProvider = Esop.class,
    mixinStandardHelpOptions = true
)
public class CommitLogBackupApplication implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(CommitLogBackupApplication.class);

    @Spec
    private CommandSpec spec;

    @Mixin
    private CassandraJMXSpec jmxSpec;

    @Mixin
    private BackupCommitLogsOperationRequest request;

    @Inject
    private OperationsService operationsService;

    public static void main(String[] args) {
        System.exit(execute(new CommitLogBackupApplication(), args));
    }

    @Override
    public void run() {
        logCommandVersionInformation(spec);

        Esop.init(this, jmxSpec, request, logger, singletonList(new CommitlogBackupModule()));

        final Operation<?> operation = operationsService.submitOperationRequest(request);

        await().forever().until(() -> operation.state.isTerminalState());

        if (operation.state == FAILED) {
            throw new IllegalStateException(format("Commitog backup operation %s was not successful.", operation.id));
        }
    }
}
