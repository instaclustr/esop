package com.instaclustr.esop.cli;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Stage;
import com.instaclustr.cassandra.CassandraModule;
import com.instaclustr.esop.guice.StorageModules;
import com.instaclustr.esop.impl.backup.BackupModules.UploadingModule;
import com.instaclustr.esop.impl.hash.HashModule;
import com.instaclustr.esop.impl.hash.HashSpec;
import com.instaclustr.esop.impl.restore.RestoreModules.DownloadingModule;
import com.instaclustr.guice.GuiceInjectorHolder;
import com.instaclustr.jackson.JacksonModule;
import com.instaclustr.operations.OperationsModule;
import com.instaclustr.picocli.CLIApplication;
import com.instaclustr.picocli.CassandraJMXSpec;
import com.instaclustr.picocli.VersionParser;
import com.instaclustr.threading.ExecutorsModule;
import jmx.org.apache.cassandra.CassandraJMXConnectionInfo;
import jmx.org.apache.cassandra.service.cassandra3.StorageServiceMBean;
import jmx.org.apache.cassandra.service.cassandra4.Cassandra4StorageServiceMBean;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Spec;

@Command(subcommands = {
    BackupApplication.class,
    RestoreApplication.class,
    CommitLogBackupApplication.class,
    CommitLogRestoreApplication.class,
    ListApplication.class,
    RemoveBackupApplication.class
},
    versionProvider = Esop.class,
    name = "esop",
    usageHelpWidth = 128,
    description = "Application for backup and restore of a Cassandra node.",
    mixinStandardHelpOptions = true
)
public class Esop extends CLIApplication implements Runnable {

    @Spec
    private CommandSpec spec;

    public static void main(String[] args) {
        main(args, true);
    }

    public static void mainWithoutExit(String[] args) {
        main(args, false);
    }

    public static void main(String[] args, boolean exit) {
        int exitCode = execute(new CommandLine(new Esop()), args);

        if (exit) {
            System.exit(exitCode);
        }
    }

    static void init(final Runnable command,
                     final CassandraJMXSpec jmxSpec,
                     final HashSpec hashSpec,
                     final List<Module> appSpecificModules) {

        final List<Module> modules = new ArrayList<>();

        if (jmxSpec != null) {
            modules.add(new CassandraModule(new CassandraJMXConnectionInfo(jmxSpec)));
        } else {
            modules.add(new AbstractModule() {
                @Override
                protected void configure() {
                    bind(StorageServiceMBean.class).toProvider(() -> null);
                    bind(Cassandra4StorageServiceMBean.class).toProvider(() -> null);
                }
            });
        }

        modules.add(new JacksonModule());
        modules.add(new OperationsModule());
        modules.add(new StorageModules());
        modules.add(new ExecutorsModule());
        modules.add(new UploadingModule());
        modules.add(new DownloadingModule());
        modules.add(new HashModule(hashSpec));
        modules.addAll(appSpecificModules);

        final Injector injector = Guice.createInjector(
            Stage.PRODUCTION, // production binds singletons as eager by default
            modules
        );

        GuiceInjectorHolder.INSTANCE.setInjector(injector);

        injector.injectMembers(command);
    }

    @Override
    public String title() {
        return "instaclustr-esop";
    }

    @Override
    public void run() {
        throw new CommandLine.ParameterException(spec.commandLine(), "Missing required parameter.");
    }

    @Override
    public String[] getVersion() throws IOException
    {
        return VersionParser.parse(title());
    }
}
