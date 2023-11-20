package com.instaclustr.esop.backup.embedded.azure;

import java.util.ArrayList;
import java.util.List;

import com.google.inject.Inject;
import com.google.inject.Module;
import com.instaclustr.esop.azure.AzureBucketService;
import com.instaclustr.esop.azure.AzureModule;
import com.instaclustr.esop.backup.embedded.BaseListingRemovalTest;
import com.instaclustr.esop.impl.StorageLocation;
import com.instaclustr.esop.impl.list.ListOperationRequest;
import com.instaclustr.io.FileUtils;
import org.testng.annotations.Test;

@Test(groups = {
        "azureTest",
        "cloudTest",
})
public class AzureListingAndBackupRemovalTest extends BaseListingRemovalTest {

    @Inject
    public AzureModule.CloudStorageAccountFactory cloudStorageAccountFactory;

    @Override
    protected List<Module> getModules() {
        return new ArrayList<Module>() {{
            add(new AzureModule());
        }};
    }

    @Override
    protected String getStorageLocation() {
        return protocol() + BUCKET_NAME + "/cluster/datacenter1/node1";
    }

    @Override
    protected String getStorageLocationForAnotherCluster() {
        return protocol() + BUCKET_NAME + "/cluster2/datacenter1/node1";
    }

    @Override
    protected String protocol() {
        return "azure://";
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
            new AzureBucketService(cloudStorageAccountFactory, request).delete(BUCKET_NAME);
        } catch (final Exception ex) {
            logger.error("Unable to remove bucket " + BUCKET_NAME);
        }
    }
}
