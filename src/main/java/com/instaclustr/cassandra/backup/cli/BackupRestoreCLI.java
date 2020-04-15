package com.instaclustr.cassandra.backup.cli;

import javax.validation.ConstraintViolation;
import javax.validation.Validation;
import javax.validation.ValidationException;
import javax.validation.Validator;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Stage;
import com.instaclustr.cassandra.CassandraModule;
import com.instaclustr.cassandra.backup.guice.StorageModules;
import com.instaclustr.guice.GuiceInjectorHolder;
import com.instaclustr.operations.OperationRequest;
import com.instaclustr.operations.OperationsModule;
import com.instaclustr.picocli.CLIApplication;
import com.instaclustr.picocli.CassandraJMXSpec;
import com.instaclustr.threading.ExecutorsModule;
import com.instaclustr.validation.GuiceInjectingConstraintValidatorFactory;
import jmx.org.apache.cassandra.CassandraJMXConnectionInfo;
import jmx.org.apache.cassandra.service.cassandra3.StorageServiceMBean;
import jmx.org.apache.cassandra.service.cassandra4.Cassandra4StorageServiceMBean;
import org.slf4j.Logger;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Spec;

@Command(subcommands = {
    BackupApplication.class,
    RestoreApplication.class,
    CommitLogBackupApplication.class,
    CommitLogRestoreApplication.class
},
    versionProvider = BackupRestoreCLI.class,
    name = "backup-restore",
    usageHelpWidth = 128,
    description = "Application for backup and restore of a Cassandra node.",
    mixinStandardHelpOptions = true
)
public class BackupRestoreCLI extends CLIApplication implements Runnable {

    @Spec
    private CommandSpec spec;

    public static void main(String[] args) {
        main(args, true);
    }

    public static void mainWithoutExit(String[] args) {
        main(args, false);
    }

    public static void main(String[] args, boolean exit) {
        int exitCode = execute(new CommandLine(new BackupRestoreCLI()), args);

        if (exit) {
            System.exit(exitCode);
        }
    }

    static void init(final Runnable command,
                     final CassandraJMXSpec jmxSpec,
                     final OperationRequest operationRequest,
                     final Logger logger,
                     final List<Module> appSpecificModules) {

        final List<Module> modules = new ArrayList<>();

        if (jmxSpec != null) {
            modules.add(new CassandraModule(new CassandraJMXConnectionInfo(jmxSpec.jmxPassword,
                                                                           jmxSpec.jmxUser,
                                                                           jmxSpec.jmxServiceURL,
                                                                           jmxSpec.trustStore,
                                                                           jmxSpec.trustStorePassword)));
        } else {
            modules.add(new AbstractModule() {
                @Override
                protected void configure() {
                    bind(StorageServiceMBean.class).toProvider(() -> null);
                    bind(Cassandra4StorageServiceMBean.class).toProvider(() -> null);
                }
            });
        }

        modules.add(new OperationsModule());
        modules.add(new StorageModules());
        modules.add(new ExecutorsModule());
        modules.addAll(appSpecificModules);

        final Injector injector = Guice.createInjector(
            Stage.PRODUCTION, // production binds singletons as eager by default
            modules
        );

        GuiceInjectorHolder.INSTANCE.setInjector(injector);

        injector.injectMembers(command);

        final Validator validator = Validation.byDefaultProvider()
            .configure()
            .constraintValidatorFactory(new GuiceInjectingConstraintValidatorFactory()).buildValidatorFactory()
            .getValidator();

        final Set<ConstraintViolation<OperationRequest>> violations = validator.validate(operationRequest);

        if (!violations.isEmpty()) {
            violations.forEach(violation -> logger.error(violation.getMessage()));
            throw new ValidationException();
        }
    }

    @Override
    public String getImplementationTitle() {
        return "backup-restore";
    }

    @Override
    public void run() {
        throw new CommandLine.ParameterException(spec.commandLine(), "Missing required parameter.");
    }
}
