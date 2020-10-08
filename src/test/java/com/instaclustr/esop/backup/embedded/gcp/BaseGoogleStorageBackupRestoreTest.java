package com.instaclustr.esop.backup.embedded.gcp;

import java.util.ArrayList;
import java.util.List;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.instaclustr.esop.backup.embedded.AbstractBackupTest;
import com.instaclustr.esop.gcp.GCPBucketService;
import com.instaclustr.esop.gcp.GCPModule;
import com.instaclustr.esop.gcp.GCPModule.GoogleStorageFactory;
import com.instaclustr.esop.impl.backup.BackupOperationRequest;
import com.instaclustr.kubernetes.KubernetesApiModule;

public abstract class BaseGoogleStorageBackupRestoreTest extends AbstractBackupTest {

    protected abstract BackupOperationRequest getBackupOperationRequest();

    public abstract GoogleStorageFactory getGoogleStorageFactory();

    public void inject() {
        final List<Module> modules = new ArrayList<Module>() {{
            add(new KubernetesApiModule());
            add(new GCPModule());
        }};

        modules.addAll(defaultModules);

        final Injector injector = Guice.createInjector(modules);
        injector.injectMembers(this);
    }

    @Override
    protected String getStorageLocation() {
        return "gcp://" + BUCKET_NAME + "/cluster/datacenter1/node1";
    }

    public void inPlaceTest(final String[][] programArguments) throws Exception {
        try {
            inPlaceBackupRestoreTest(programArguments);
        } finally {
            new GCPBucketService(getGoogleStorageFactory(), getBackupOperationRequest()).delete(BUCKET_NAME);
        }
    }

    public void liveCassandraTest(final String[][] programArguments, final String cassandraVersion) throws Exception {
        try {
            liveBackupRestoreTest(programArguments, cassandraVersion);
        } finally {
            new GCPBucketService(getGoogleStorageFactory(), getBackupOperationRequest()).delete(BUCKET_NAME);
        }
    }
}
