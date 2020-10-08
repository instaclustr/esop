package com.instaclustr.esop.backup.embedded.s3;

import java.util.ArrayList;
import java.util.List;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.instaclustr.esop.backup.embedded.AbstractBackupTest;
import com.instaclustr.esop.impl.backup.BackupOperationRequest;
import com.instaclustr.esop.s3.aws.S3BucketService;
import com.instaclustr.esop.s3.aws.S3Module;
import com.instaclustr.esop.s3.aws.S3Module.S3TransferManagerFactory;
import com.instaclustr.kubernetes.KubernetesApiModule;

public abstract class BaseS3BackupRestoreTest extends AbstractBackupTest {

    protected abstract BackupOperationRequest getBackupOperationRequest();

    public void inject() {
        final List<Module> modules = new ArrayList<Module>() {{
            add(new KubernetesApiModule());
            add(new S3Module());
        }};

        modules.addAll(defaultModules);

        final Injector injector = Guice.createInjector(modules);
        injector.injectMembers(this);
    }

    @Override
    protected String getStorageLocation() {
        return "s3://" + BUCKET_NAME + "/cluster/datacenter1/node1";
    }

    public void inPlaceTest(final String[][] programArguments) throws Exception {
        try {
            inPlaceBackupRestoreTest(programArguments);
        } finally {
            new S3BucketService(getTransferManagerFactory(), getBackupOperationRequest()).delete(BUCKET_NAME);
        }
    }

    public void liveCassandraTest(final String[][] programArguments, final String cassandraVersion) throws Exception {
        try {
            liveBackupRestoreTest(programArguments, cassandraVersion);
        } finally {
            new S3BucketService(getTransferManagerFactory(), getBackupOperationRequest()).delete(BUCKET_NAME);
        }
    }

    public abstract S3TransferManagerFactory getTransferManagerFactory();
}
