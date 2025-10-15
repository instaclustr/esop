package com.instaclustr.esop.cli;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.google.inject.Module;
import com.instaclustr.esop.guice.RestorerFactory;
import com.instaclustr.esop.impl.hash.HashSpec;
import com.instaclustr.esop.impl.remove.RemoveBackupModule;
import com.instaclustr.esop.impl.remove.RemoveBackupOperation;
import com.instaclustr.esop.impl.remove.RemoveBackupRequest;
import com.instaclustr.measure.Time;
import com.instaclustr.operations.Operation;
import com.instaclustr.operations.OperationsService;
import com.instaclustr.picocli.CassandraJMXSpec;
import com.instaclustr.picocli.typeconverter.TimeMeasureTypeConverter;
import com.instaclustr.scheduling.DaemonScheduler;
import jmx.org.apache.cassandra.service.CassandraJMXService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Spec;

import static com.instaclustr.operations.Operation.State.FAILED;
import static com.instaclustr.picocli.CLIApplication.execute;
import static java.lang.String.format;
import static org.awaitility.Awaitility.await;

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

    @Option(names = {"-r", "--rate"},
        description = "Rate of operation executon.",
        converter = TimeMeasureTypeConverter.class)
    public Time rate = Time.zeroTime();

    @Inject
    private OperationsService operationsService;

    @Inject
    private CassandraJMXService cassandraJMXService;

    @Inject
    private Map<String, RestorerFactory> restorerFactoryMap;

    @Inject
    private ObjectMapper objectMapper;

    public static void main(String[] args) {
        System.exit(execute(new ListApplication(), args));
    }

    @Override
    public void run() {
        List<Module> additionalModules = new ArrayList<>(Esop.getStorageSpecificModules());
        additionalModules.add(new RemoveBackupModule());

        Esop.init(this, jmxSpec, new HashSpec(), additionalModules);

        if (rate.value == 0) {
            final Operation<?> operation = operationsService.submitOperationRequest(request);

            await().forever().until(() -> operation.state.isTerminalState());

            if (operation.state == FAILED) {
                throw new IllegalStateException(format("Remove operation %s was not successful.", operation.id));
            }
        } else {
            final Supplier<RemoveBackupOperation> supplier = () -> new RemoveBackupOperation(request, cassandraJMXService, restorerFactoryMap, objectMapper);
            final DaemonScheduler<RemoveBackupRequest, RemoveBackupOperation> scheduler = new DaemonScheduler<>(rate, supplier);
            scheduler.setup();
            scheduler.execute();
        }
    }
}

