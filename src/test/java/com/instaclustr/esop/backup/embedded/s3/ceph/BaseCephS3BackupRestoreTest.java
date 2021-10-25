package com.instaclustr.esop.backup.embedded.s3.ceph;

import java.util.ArrayList;
import java.util.List;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.instaclustr.esop.backup.embedded.AbstractBackupTest;
import com.instaclustr.esop.impl.backup.BackupOperationRequest;
import com.instaclustr.esop.s3.ceph.CephBucketService;
import com.instaclustr.esop.s3.ceph.CephModule;
import com.instaclustr.esop.s3.ceph.CephModule.CephS3TransferManagerFactory;
import com.instaclustr.kubernetes.KubernetesApiModule;

public abstract class BaseCephS3BackupRestoreTest extends AbstractBackupTest {

    protected abstract BackupOperationRequest getBackupOperationRequest();

    @Override
    protected String protocol() {
        return "ceph://";
    }

    public void inject() {
        final List<Module> modules = new ArrayList<Module>() {{
            add(new KubernetesApiModule());
            add(new CephModule());
        }};

        modules.addAll(defaultModules);

        final Injector injector = Guice.createInjector(modules);
        injector.injectMembers(this);
    }

    public void inPlaceTest(final String[][] programArguments) throws Exception {
        try {
            inPlaceBackupRestoreTest(programArguments);
        } finally {
            new CephBucketService(getTransferManagerFactory(), getBackupOperationRequest()).delete(BUCKET_NAME);
        }
    }

    public void liveCassandraTest(final String[][] programArguments, final String cassandraVersion) throws Exception {
        try {
            liveBackupRestoreTest(programArguments, cassandraVersion);
        } finally {
            new CephBucketService(getTransferManagerFactory(), getBackupOperationRequest()).delete(BUCKET_NAME);
        }
    }

    public abstract CephS3TransferManagerFactory getTransferManagerFactory();
}
