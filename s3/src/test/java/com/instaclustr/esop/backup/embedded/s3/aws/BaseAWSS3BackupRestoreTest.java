package com.instaclustr.esop.backup.embedded.s3.aws;

import java.util.ArrayList;
import java.util.List;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.instaclustr.esop.backup.embedded.AbstractBackupTest;
import com.instaclustr.esop.impl.BucketService;
import com.instaclustr.esop.impl.backup.BackupOperationRequest;

public abstract class BaseAWSS3BackupRestoreTest extends AbstractBackupTest {

    protected abstract BackupOperationRequest getBackupOperationRequest();

    @Override
    protected String protocol() {
        return "s3://";
    }

    public void inject(AbstractModule s3Module) {
        final List<Module> modules = new ArrayList<Module>() {{
            add(s3Module);
        }};

        modules.addAll(defaultModules);

        final Injector injector = Guice.createInjector(modules);
        injector.injectMembers(this);
    }

    public void inPlaceTest(final String[][] programArguments) throws Exception {
        try {
            inPlaceBackupRestoreTest(programArguments);
        } finally {
            deleteBucket();
        }
    }

    public void liveCassandraTest(final String[][] programArguments) throws Exception {
        try {
            liveBackupRestoreTest(programArguments, 1);
        } finally {
            deleteBucket();
        }
    }

    public abstract void deleteBucket() throws BucketService.BucketServiceException;
}
