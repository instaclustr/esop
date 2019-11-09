package com.instaclustr.cassandra.backup.real;

import static com.datastax.driver.core.querybuilder.QueryBuilder.insertInto;
import static com.instaclustr.cassandra.backup.real.TestEntity.DATE;
import static com.instaclustr.cassandra.backup.real.TestEntity.ID;
import static com.instaclustr.cassandra.backup.real.TestEntity.KEYSPACE;
import static com.instaclustr.cassandra.backup.real.TestEntity.NAME;
import static com.instaclustr.cassandra.backup.real.TestEntity.TABLE;
import static java.lang.String.format;

import javax.management.remote.JMXServiceURL;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.regex.Pattern;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.utils.UUIDs;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.instaclustr.cassandra.backup.impl.backup.BackupOperation.TakeSnapshotOperation;
import com.instaclustr.cassandra.backup.impl.backup.BackupOperationRequest;
import com.instaclustr.picocli.typeconverter.CassandraJMXServiceURLTypeConverter;
import jmx.org.apache.cassandra.JMXConnectionInfo;
import jmx.org.apache.cassandra.guice.CassandraModule;
import jmx.org.apache.cassandra.service.StorageServiceMBean;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractBackupRestoreTest {

    protected static final Logger logger = LoggerFactory.getLogger(AbstractBackupRestoreTest.class);

    @Inject
    protected StorageServiceMBean storageServiceMBean;

    protected Cluster cluster;

    protected Session session;

    protected MappingManager mappingManager;

    protected Mapper<TestEntity> testEntityMapper;

    protected String target = new File("target").toPath().toAbsolutePath().toString();

    protected String cassandraDir = new File("target/cassandra").toPath().toAbsolutePath().toString();

    protected String cassandraRestoredDir = new File("target/cassandra-restored").toPath().toAbsolutePath().toString();

    protected String cassandraRestoredConfigDir = new File("target/cassandra-restored/conf").toPath().toAbsolutePath().toString();

    protected String lockFile = target("lock");

    protected final Pattern commitlogPattern = Pattern.compile(".*(CommitLog-\\d+-\\d+\\.log)\\.(\\d+)");

    public String target(final String path) {
        return Paths.get(target).resolve(path).toAbsolutePath().toString();
    }

    public File targetFile(final String path) {
        return Paths.get(target).resolve(path).toAbsolutePath().toFile();
    }

    public Path targetPath(final String path) {
        return Paths.get(target).resolve(path).toAbsolutePath();
    }

    public String resource(final String path) {
        return resourceFile(path).toPath().toAbsolutePath().toString();
    }

    public File resourceFile(final String path) {
        return new File("src/test/resources/" + path);
    }

    public Path resourcePath(final String path) {
        return resourceFile(path).toPath();
    }

    // utils

    protected void takeSnapshot(final BackupOperationRequest request) {
        new TakeSnapshotOperation(storageServiceMBean,
                                  new TakeSnapshotOperation.TakeSnapshotOperationRequest(request.keyspaces,
                                                                                         request.snapshotTag,
                                                                                         request.table)).run();
    }

    protected void disableAutoCompaction(final String keyspace) {
        try {
            storageServiceMBean.disableAutoCompaction(keyspace);
        } catch (final Exception ex) {
            logger.error(format("Failed to disable auto compaction on keyspace %s", keyspace), ex);
        }
    }

    protected void flush(final String keyspace) {
        try {
            storageServiceMBean.forceKeyspaceFlush(keyspace);
        } catch (final Exception ex) {
            logger.error(format("Failed to flush keyspace %s", keyspace), ex);
        }
    }

    protected abstract void createSchema(final Session session);

    protected abstract void deleteSchema(final Session session);

    protected abstract void cleanup();

    protected void deleteResources(String... resources) {
        for (final String resource : resources) {
            try {
                FileUtils.deleteDirectory(new File("src/test/resources/", resource));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    protected void injectMembers() {
        final CassandraJMXServiceURLTypeConverter converter = new CassandraJMXServiceURLTypeConverter();
        final JMXServiceURL jmxServiceURL = converter.convert("127.0.0.1:7199");

        final List<Module> modules = new ArrayList<Module>() {{
            add(new CassandraModule(new JMXConnectionInfo(null,
                                                          null,
                                                          jmxServiceURL,
                                                          null,
                                                          null)));
        }};

        final Injector injector = Guice.createInjector(modules);

        injector.injectMembers(this);
    }

    protected Cluster initCluster() {
        return Cluster.builder().withPort(9042).addContactPoint("127.0.0.1").build();
    }

    protected long insert(final int id) {

        final UUID timeBased = UUIDs.timeBased();

        if (session == null) {
            System.out.println("session is null");
        }

        try {
            session.execute(insertInto(KEYSPACE, TABLE)
                                .values(new String[]{ID, DATE, NAME},
                                        new Object[]{id, timeBased, "stefan1"}));
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            logger.error("Exception while sleeping!");
        }

        return System.currentTimeMillis();
    }

    protected Date uuidToDate(final UUID uuid) {
        return new Date(uuid.timestamp() / 10000L - 12219292800000L);
    }

    protected void createCassandraDirStructure(final String root) throws IOException {

        final Path rootPath = Paths.get(root);

        FileUtils.deleteDirectory(rootPath.toFile());

        Files.createDirectory(rootPath);

        Files.createDirectory(rootPath.resolve("bin"));
        Files.createDirectory(rootPath.resolve("commitlog"));
        Files.createDirectory(rootPath.resolve("conf"));
        Files.createDirectory(rootPath.resolve("data"));
        Files.createDirectory(rootPath.resolve("hints"));
        Files.createDirectory(rootPath.resolve("saved_caches"));
    }
}
