package com.instaclustr.cassandra.backup.embedded.azure;

import java.util.ArrayList;
import java.util.List;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.instaclustr.cassandra.backup.azure.AzureBucketService;
import com.instaclustr.cassandra.backup.azure.AzureModule;
import com.instaclustr.cassandra.backup.azure.AzureModule.CloudStorageAccountFactory;
import com.instaclustr.cassandra.backup.embedded.AbstractBackupTest;
import com.instaclustr.cassandra.backup.impl.backup.BackupOperationRequest;
import com.instaclustr.kubernetes.KubernetesApiModule;

public abstract class BaseAzureBackupRestoreTest extends AbstractBackupTest {

    protected abstract BackupOperationRequest getBackupOperationRequest();

    public abstract CloudStorageAccountFactory getStorageAccountFactory();

    public void inject() {
        final List<Module> modules = new ArrayList<Module>() {{
            add(new KubernetesApiModule());
            add(new AzureModule());
        }};

        modules.addAll(defaultModules);

        final Injector injector = Guice.createInjector(modules);
        injector.injectMembers(this);
    }

    @Override
    protected String getStorageLocation() {
        return "azure://" + BUCKET_NAME + "/cluster/datacenter1/node1";
    }

    public void inPlaceTest(final String[][] programArguments) throws Exception {
        try {
            inPlaceBackupRestoreTest(programArguments);
        } finally {
            new AzureBucketService(getStorageAccountFactory(), getBackupOperationRequest()).delete(BUCKET_NAME);
        }
    }

    public void liveCassandraTest(final String[][] programArguments, String cassandraVersion) throws Exception {
        try {
            liveBackupRestoreTest(programArguments, cassandraVersion);
        } finally {
            new AzureBucketService(getStorageAccountFactory(), getBackupOperationRequest()).delete(BUCKET_NAME);
        }
    }
}
