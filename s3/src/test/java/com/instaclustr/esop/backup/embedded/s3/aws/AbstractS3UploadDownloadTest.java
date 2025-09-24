package com.instaclustr.esop.backup.embedded.s3.aws;

import java.util.ArrayList;
import java.util.List;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.instaclustr.esop.backup.embedded.AbstractBackupTest;

public abstract class AbstractS3UploadDownloadTest extends AbstractBackupTest {
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
}
