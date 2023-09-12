package com.instaclustr.esop.backup.embedded.local;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.instaclustr.esop.backup.embedded.AbstractBackupTest;
import com.instaclustr.esop.impl.StorageLocation;
import com.instaclustr.esop.impl.restore.RestoreOperationRequest;
import com.instaclustr.esop.local.LocalFileModule;
import com.instaclustr.io.FileUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.instaclustr.esop.impl.restore.RestorationStrategy.RestorationStrategyType.HARDLINKS;
import static com.instaclustr.esop.impl.restore.RestorationStrategy.RestorationStrategyType.IMPORT;
import static com.instaclustr.io.FileUtils.deleteDirectory;

public abstract class AbstractLocalBackupTest extends AbstractBackupTest {

    @Override
    protected String protocol() {
        return "file://";
    }

    @BeforeMethod
    public void setup() {

        final List<Module> modules = new ArrayList<Module>() {{
            add(new LocalFileModule());
        }};

        modules.addAll(defaultModules);

        final Injector injector = Guice.createInjector(modules);
        injector.injectMembers(this);

        init();
    }

    @AfterMethod
    public void teardown() throws Exception {
        destroy();
    }

    @Test
    public void testInPlaceBackupRestore() throws Exception {
        inPlaceBackupRestoreTest(inPlaceArguments());
    }

    @Test
    public void testImportingBackupAndRestoreRenameEntitiesCrossKeyspaces() throws Exception {
        liveBackupRestoreTestRenamedEntities(importArgumentsRenamedTable(IMPORT, true), 2, true);
    }

    @Test
    public void testImportingHardlinksBackupAndRestoreRenameEntitiesCrossKeyspaces() throws Exception {
        liveBackupRestoreTestRenamedEntities(importArgumentsRenamedTable(HARDLINKS, true),2, true);
    }

    @Test
    public void testImportingBackupAndRestoreRenameEntities() throws Exception {
        liveBackupRestoreTestRenamedEntities(importArgumentsRenamedTable(IMPORT, false),2, false);
    }

    @Test
    public void testImportingHardlinksBackupAndRestoreRenameEntities() throws Exception {
        liveBackupRestoreTestRenamedEntities(importArgumentsRenamedTable(HARDLINKS, false),2, false);
    }

    @Test
    public void testImportingBackupAndRestore() throws Exception {
        liveBackupRestoreTest(importArguments(), 2);
    }

    @Test
    public void testHardlinksBackupAndRestore() throws Exception {
        liveBackupRestoreTest(hardlinkingArguments(), 2);
    }

    @Test
    public void testImportingOnDifferentSchema() throws Exception {
        liveBackupWithRestoreOnDifferentSchema(restoreByImportingIntoDifferentSchemaArguments(IMPORT));
    }

    @Test
    public void testHardlinksOnDifferentSchema() throws Exception {
        liveBackupWithRestoreOnDifferentSchema(restoreByImportingIntoDifferentSchemaArguments(HARDLINKS));
    }

    @Test
    public void testImportingOnDifferentTableSchemaAddColumn() throws Exception {
        liveBackupWithRestoreOnDifferentTableSchema(restoreByImportingIntoDifferentSchemaArguments(IMPORT),true);
    }

    @Test
    public void testHardlinksOnDifferentTableSchemaAddColumn() throws Exception {
        liveBackupWithRestoreOnDifferentTableSchema(restoreByImportingIntoDifferentSchemaArguments(HARDLINKS),true);
    }

    @Test
    public void testImportingOnDifferentTableSchemaDropColumn() throws Exception {
        liveBackupWithRestoreOnDifferentTableSchema(restoreByImportingIntoDifferentSchemaArguments(IMPORT),false);
    }

    @Test
    public void testHardlinksOnDifferentTableSchemaDropColumn() throws Exception {
        liveBackupWithRestoreOnDifferentTableSchema(restoreByImportingIntoDifferentSchemaArguments(HARDLINKS),false);
    }

    @Test
    public void testDownload() throws Exception {
        try {
            RestoreOperationRequest restoreOperationRequest = new RestoreOperationRequest();

            FileUtils.createDirectory(Paths.get(target("backup1") + "/cluster/test-dc/1/manifests").toAbsolutePath());

            restoreOperationRequest.storageLocation = new StorageLocation("file://" + target("backup1") + "/cluster/test-dc/1");

            Files.write(Paths.get("target/backup1/cluster/test-dc/1/manifests/snapshot-name-" + UUID.randomUUID()).toAbsolutePath(),
                        "hello".getBytes(),
                        StandardOpenOption.CREATE_NEW
            );

        } finally {
            deleteDirectory(Paths.get(target("backup1")));
            deleteDirectory(Paths.get(target("commitlog_download_dir")));
        }
    }

    @Override
    protected String getStorageLocation() {
        return "file://" + target("backup1") + "/cluster/datacenter1/node1";
    }
    @Override
    protected String getStorageLocationForAnotherCluster() {
        return "file://" + target("backup1") + "/cluster2/datacenter1/node1";
    }
}
