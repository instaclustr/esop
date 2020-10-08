package com.instaclustr.esop.impl._import;

import javax.validation.constraints.NotNull;
import java.nio.file.Path;
import java.nio.file.Paths;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.ext.NioPathDeserializer;
import com.fasterxml.jackson.databind.ext.NioPathSerializer;
import com.google.common.base.MoreObjects;
import com.instaclustr.operations.OperationRequest;
import com.instaclustr.picocli.typeconverter.PathTypeConverter;
import picocli.CommandLine.Option;

/**
 * <pre>
 * {@code
 * $ nodetool help import
 * NAME
 *         nodetool import - Import new SSTables to the system
 *
 * SYNOPSIS
 *         nodetool [(-h <host> | --host <host>)] [(-p <port> | --port <port>)]
 *                 [(-pp | --print-port)] [(-pw <password> | --password <password>)]
 *                 [(-pwf <passwordFilePath> | --password-file <passwordFilePath>)]
 *                 [(-u <username> | --username <username>)] import
 *                 [(-c | --no-invalidate-caches)] [(-e | --extended-verify)]
 *                 [(-l | --keep-level)] [(-q | --quick)] [(-r | --keep-repaired)]
 *                 [(-t | --no-tokens)] [(-v | --no-verify)] [--] <keyspace> <table>
 *                 <directory> ...
 *
 * OPTIONS
 *         -c, --no-invalidate-caches
 *             Don't invalidate the row cache when importing
 *
 *         -e, --extended-verify
 *             Run an extended verify, verifying all values in the new sstables
 *
 *         -h <host>, --host <host>
 *             Node hostname or ip address
 *
 *         -l, --keep-level
 *             Keep the level on the new sstables
 *
 *         -p <port>, --port <port>
 *             Remote jmx agent port number
 *
 *         -pp, --print-port
 *             Operate in 4.0 mode with hosts disambiguated by port number
 *
 *         -pw <password>, --password <password>
 *             Remote jmx agent password
 *
 *         -pwf <passwordFilePath>, --password-file <passwordFilePath>
 *             Path to the JMX password file
 *
 *         -q, --quick
 *             Do a quick import without verifying sstables, clearing row cache or
 *             checking in which data directory to put the file
 *
 *         -r, --keep-repaired
 *             Keep any repaired information from the sstables
 *
 *         -t, --no-tokens
 *             Don't verify that all tokens in the new sstable are owned by the
 *             current node
 *
 *         -u <username>, --username <username>
 *             Remote jmx agent username
 *
 *         -v, --no-verify
 *             Don't verify new sstables
 *
 *         --
 *             This option can be used to separate command-line options from the
 *             list of argument, (useful when arguments might be mistaken for
 *             command-line options
 *
 *         <keyspace> <table> <directory> ...
 *             The keyspace, table name and directories to import sstables from
 * }</pre>
 */
public class ImportOperationRequest extends OperationRequest {

    private static final Path defaultPath = Paths.get("/tmp/cassandra-import-source");

    // for keyspace and table, they are not marked as required because on CLI path, we derive this information from 'entities'
    // there are two REST paths, the first one is pure "import" operation, the second one is restore operation.
    // on "import", we are checking that both fields are not null

    @Option(names = {"--import-keyspace"},
        description = "keyspace to import",
        required = false) // not required as we will look into "entities" of restore request
    public String keyspace;

    @Option(names = {"--import-table"},
        description = "table to import",
        required = false)  // not required as we will look into "entities" of restore request
    public String table;

    @Option(names = {"--import-keep-level"},
        description = "upon import, keep the level on the new sstables")
    public boolean keepLevel = false;

    @Option(names = {"--import-keep-repaired"},
        description = "upon import, keep any repaired information from the SSTables")
    public boolean keepRepaired = false;

    @Option(names = {"--import-no-verify"},
        description = "upon import, do not verify new SSTables")
    public boolean noVerify = false;

    @Option(names = {"--import-no-verify-tokens"},
        description = "upon import, do not verify that all tokens in the new SSTable are owned by the current node")
    public boolean noVerifyTokens = false;

    @Option(names = {"--import-no-invalidate-caches"},
        description = "upon import, do not invalidate the row cache when importing")
    public boolean noInvalidateCaches = false;

    @Option(names = {"--import-quick"},
        description = "upon import, do a quick import without verifying SSTables, clearing row cache or checking in which data directory to put the file")
    public boolean quick = false;

    @Option(names = {"--import-extended-verify"},
        description = "upon import, run an extended verify, verifying all values in the new sstables")
    public boolean extendedVerify = false;

    @NotNull
    @JsonDeserialize(using = NioPathDeserializer.class)
    @JsonSerialize(using = NioPathSerializer.class)
    @Option(names = {"--import-source-dir"},
        description = "Directory from which SSTables are taken",
        converter = PathTypeConverter.class,
        defaultValue = "/tmp/cassandra-import-source")
    public Path sourceDir = defaultPath;

    public ImportOperationRequest() {
        // for picocli
        this(true);
    }

    public ImportOperationRequest(final boolean quick) {
        if (quick) {
            quickImport();
        }
        type = "import";
    }

    public ImportOperationRequest(final String keyspace,
                                  final String table,
                                  final Path sourceDir) {
        this(true);
        this.type = "import";
        this.keyspace = keyspace;
        this.table = table;
        this.sourceDir = sourceDir;
    }

    @JsonCreator
    public ImportOperationRequest(@JsonProperty("type") final String type,
                                  @JsonProperty("keyspace") final String keyspace,
                                  @JsonProperty("table") final String table,
                                  @JsonProperty("keepLevel") final boolean keepLevel,
                                  @JsonProperty("noVerify") final boolean noVerify,
                                  @JsonProperty("noVerifyTokens") final boolean noVerifyTokens,
                                  @JsonProperty("noInvalidateCaches") final boolean noInvalidateCaches,
                                  @JsonProperty("quick") final boolean quick,
                                  @JsonProperty("extendedVerify") final boolean extendedVerify,
                                  @NotNull
                                  @JsonProperty("sourceDir")
                                  @JsonDeserialize(using = NioPathDeserializer.class)
                                  @JsonSerialize(using = NioPathSerializer.class) final Path sourceDir) {
        this.keyspace = keyspace;
        this.table = table;
        this.keepLevel = keepLevel;
        this.noVerify = noVerify;
        this.noVerifyTokens = noVerifyTokens;
        this.noInvalidateCaches = noInvalidateCaches;
        this.quick = quick;
        this.extendedVerify = extendedVerify;
        this.sourceDir = sourceDir;
        this.type = "import";

        if (quick) {
            quickImport();
        }
    }

    private void quickImport() {
        this.noVerifyTokens = true;
        this.noInvalidateCaches = true;
        this.noVerify = true;
        this.extendedVerify = false;
    }

    public ImportOperationRequest copy() {
        return copy(this.keyspace, this.table);
    }

    public ImportOperationRequest copy(final String keyspace, final String table) {
        return new ImportOperationRequest(type,
                                          keyspace,
                                          table,
                                          this.keepLevel,
                                          this.noVerify,
                                          this.noVerifyTokens,
                                          this.noInvalidateCaches,
                                          this.quick,
                                          this.extendedVerify,
                                          this.sourceDir);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("keyspace", keyspace)
            .add("table", table)
            .add("keepLevel", keepLevel)
            .add("noVerify", noVerify)
            .add("noVerifyTokens", noVerifyTokens)
            .add("noInvalidateCaches", noInvalidateCaches)
            .add("quick", quick)
            .add("extendedVerify", extendedVerify)
            .add("sourceDir", sourceDir)
            .toString();
    }
}
