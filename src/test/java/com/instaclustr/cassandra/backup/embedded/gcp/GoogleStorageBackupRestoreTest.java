package com.instaclustr.cassandra.backup.embedded.gcp;

import java.util.ArrayList;
import java.util.List;

import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.instaclustr.cassandra.backup.gcp.GCPModule;
import com.instaclustr.cassandra.backup.gcp.GCPModule.GoogleStorageFactory;
import com.instaclustr.cassandra.backup.impl.backup.BackupOperationRequest;
import com.instaclustr.kubernetes.KubernetesApiModule;
import com.instaclustr.threading.ExecutorsModule;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = {
    "googleTest",
    "cloudTest",
})
public class GoogleStorageBackupRestoreTest extends BaseGoogleStorageBackupRestoreTest {

    @Inject
    public GoogleStorageFactory googleStorageFactory;

    @BeforeMethod
    public void setup() throws Exception {

        final List<Module> modules = new ArrayList<Module>() {{
            add(new KubernetesApiModule());
            add(new GCPModule());
            add(new ExecutorsModule());
        }};

        final Injector injector = Guice.createInjector(modules);
        injector.injectMembers(this);

        init();
    }

    @AfterMethod
    public void teardown() throws Exception {
        destroy();
    }

    protected BackupOperationRequest getBackupOperationRequest() {
        return new BackupOperationRequest();
    }

    @Override
    public GoogleStorageFactory getGoogleStorageFactory() {
        return googleStorageFactory;
    }

    @Test
    public void testInPlaceBackupRestore() throws Exception {
        inPlaceTest(inPlaceArguments());
    }

    @Test
    public void testImportingBackupAndRestore() throws Exception {
        liveCassandraTest(importArguments());
    }

    @Test
    public void testHardlinkingBackupAndRestore() throws Exception {
        liveCassandraTest(hardlinkingArguments());
    }

}
