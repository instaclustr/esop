package com.instaclustr.esop.backup.embedded.s3.aws;

import com.google.inject.Inject;
import com.google.inject.Module;
import com.instaclustr.esop.backup.embedded.BaseListingRemovalTest;
import com.instaclustr.esop.impl.StorageLocation;
import com.instaclustr.esop.impl.list.ListOperationRequest;
import com.instaclustr.esop.s3.aws.S3BucketService;
import com.instaclustr.esop.s3.aws.S3Module;
import com.instaclustr.io.FileUtils;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

@Test(groups = {
        "s3Test",
        "cloudTest",
})
public class S3ListingAndBackupRemovalTest extends BaseListingRemovalTest {

    @Inject
    public S3Module.S3TransferManagerFactory transferManagerFactory;

    @Override
    protected List<Module> getModules() {
        return new ArrayList<Module>() {{
            add(new S3Module());
        }};
    }

    @Override
    protected String protocol() {
        return "s3://";
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
            new S3BucketService(transferManagerFactory, request).delete(BUCKET_NAME);
        } catch (final Exception ex) {
            logger.error("Unable to remove bucket " + BUCKET_NAME);
        }
    }
}