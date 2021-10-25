package com.instaclustr.esop.backup.embedded.azure;

import java.util.ArrayList;
import java.util.List;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.instaclustr.esop.azure.AzureBucketService;
import com.instaclustr.esop.azure.AzureModule;
import com.instaclustr.esop.azure.AzureModule.CloudStorageAccountFactory;
import com.instaclustr.esop.backup.embedded.AbstractBackupTest;
import com.instaclustr.esop.impl.backup.BackupOperationRequest;
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
    protected String protocol() {
        return "azure://";
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
