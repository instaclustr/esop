package com.instaclustr.esop.backup.embedded.local;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import com.google.inject.Module;
import com.instaclustr.esop.backup.embedded.BaseListingRemovalTest;
import com.instaclustr.esop.local.LocalFileModule;
import com.instaclustr.io.FileUtils;
import org.testng.annotations.Test;

import static com.instaclustr.io.FileUtils.deleteDirectory;

@Test(groups = {
        "localTest",
})
public class LocalListingAndBackupRemovalTest extends BaseListingRemovalTest {

    @Override
    protected List<Module> getModules() {
        return new ArrayList<Module>() {{
            add(new LocalFileModule());
        }};
    }

    @Override
    protected String getStorageLocation() {
        return protocol() + target(BUCKET_NAME) + "/cluster/datacenter1/node1";
    }

    @Override
    protected String getStorageLocationForAnotherCluster() {
        return protocol() + target(BUCKET_NAME) + "/cluster2/datacenter1/node1";
    }

    @Override
    protected String protocol() {
        return "file://";
    }

    @Override
    protected void destroy() throws Exception {
        FileUtils.deleteDirectory(cassandraDir);
        deleteDirectory(Paths.get(target(BUCKET_NAME)));
    }
}
