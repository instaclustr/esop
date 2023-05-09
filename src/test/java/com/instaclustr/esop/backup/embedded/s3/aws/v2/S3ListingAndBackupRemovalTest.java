package com.instaclustr.esop.backup.embedded.s3.aws.v2;

import java.util.ArrayList;
import java.util.List;

import com.google.inject.Inject;
import com.google.inject.Module;
import com.instaclustr.esop.backup.embedded.BaseListingRemovalTest;
import com.instaclustr.esop.impl.StorageLocation;
import com.instaclustr.esop.impl.list.ListOperationRequest;
import com.instaclustr.esop.s3.aws_v2.S3BucketService;
import com.instaclustr.esop.s3.aws_v2.S3V2Module;
import com.instaclustr.esop.s3.v2.S3ClientsFactory.S3Clients;
import com.instaclustr.io.FileUtils;
import org.testng.annotations.Test;

@Test(groups = {
        "s3Test",
        "cloudTest",
})
public class S3ListingAndBackupRemovalTest extends BaseListingRemovalTest {

    @Inject
    public S3Clients s3Clients;

    @Override
    protected List<Module> getModules() {
        return new ArrayList<Module>() {{
            add(new S3V2Module());
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
            new S3BucketService(s3Clients).delete(BUCKET_NAME);
        } catch (final Exception ex) {
            logger.error("Unable to remove bucket " + BUCKET_NAME);
        }
    }
}