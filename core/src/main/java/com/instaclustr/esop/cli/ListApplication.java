package com.instaclustr.esop.cli;

import java.util.ArrayList;
import java.util.List;

import com.google.inject.Inject;
import com.google.inject.Module;
import com.instaclustr.esop.impl.hash.HashSpec;
import com.instaclustr.esop.impl.list.ListModule;
import com.instaclustr.esop.impl.list.ListOperationRequest;
import com.instaclustr.operations.Operation;
import com.instaclustr.operations.OperationsService;
import com.instaclustr.picocli.CassandraJMXSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Spec;

import static com.instaclustr.operations.Operation.State.FAILED;
import static com.instaclustr.picocli.CLIApplication.execute;
import static java.lang.String.format;
import static org.awaitility.Awaitility.await;

@Command(name = "list",
    description = "lists remote storage to see what backups there are",
    sortOptions = false,
    versionProvider = Esop.class,
    mixinStandardHelpOptions = true
)
public class ListApplication implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(ListApplication.class);

    @Spec
    private CommandSpec spec;

    @Mixin
    private ListOperationRequest request;

    @Mixin
    private CassandraJMXSpec jmxSpec;

    @Inject
    private OperationsService operationsService;

    public static void main(String[] args) {
        System.exit(execute(new ListApplication(), args));
    }

    @Override
    public void run() {
        List<Module> additionalModules = new ArrayList<>(Esop.getStorageSpecificModules());
        additionalModules.add(new ListModule());

        Esop.init(this, jmxSpec, new HashSpec(), additionalModules);

        final Operation<?> operation = operationsService.submitOperationRequest(request);

        await().forever().until(() -> operation.state.isTerminalState());

        if (operation.state == FAILED) {
            throw new IllegalStateException(format("List operation %s was not successful.", operation.id));
        }
    }
}
