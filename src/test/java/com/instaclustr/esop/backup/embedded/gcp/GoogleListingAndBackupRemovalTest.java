package com.instaclustr.esop.backup.embedded.gcp;

import com.google.inject.Inject;
import com.google.inject.Module;
import com.instaclustr.esop.backup.embedded.BaseListingRemovalTest;
import com.instaclustr.esop.gcp.GCPBucketService;
import com.instaclustr.esop.gcp.GCPModule;
import com.instaclustr.esop.impl.StorageLocation;
import com.instaclustr.esop.impl.list.ListOperationRequest;
import com.instaclustr.io.FileUtils;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

@Test(groups = {
        "googleTest",
        "cloudTest",
})
public class GoogleListingAndBackupRemovalTest extends BaseListingRemovalTest {

    @Inject
    public GCPModule.GoogleStorageFactory storageFactory;

    @Override
    protected List<Module> getModules() {
        return new ArrayList<Module>() {{
            add(new GCPModule());
        }};
    }

    @Override
    protected String getStorageLocation() {
        return protocol() + "://" + BUCKET_NAME + "/cluster/datacenter1/node1";
    }

    @Override
    protected String protocol() {
        return "gcp";
    }

    @Override
    public void destroy() throws Exception {
        try {
            FileUtils.deleteDirectory(cassandraDir);
        } catch (final Exception ex) {
            logger.error("Unable to remove Cassandra dir " + cassandraDir);
        }

        try {
            ListOperationRequest request = new ListOperationRequest();
            request.storageLocation = new StorageLocation(getStorageLocation());
            new GCPBucketService(storageFactory, request).delete(BUCKET_NAME);
        } catch (final Exception ex) {
            logger.error("Unable to remove bucket " + BUCKET_NAME);
        }
    }
}