package com.instaclustr.esop.backup.embedded.s3;

import java.util.HashMap;

import com.google.inject.Inject;
import com.instaclustr.esop.impl.backup.BackupOperationRequest;
import com.instaclustr.esop.s3.aws.S3Module.S3TransferManagerFactory;
import com.instaclustr.kubernetes.KubernetesService;
import io.kubernetes.client.ApiException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Ignore;
import org.testng.annotations.Test;

@Test(groups = {
    "s3Test",
    "k8sTest"
})
public class KubernetesS3BackupRestoreTest extends BaseS3BackupRestoreTest {

    @Inject
    public KubernetesService kubernetesService;

    @Inject
    public S3TransferManagerFactory transferManagerFactory;

    @BeforeMethod
    public void setup() throws ApiException {
        inject();
        init();
    }

    @AfterMethod
    public void teardown() throws ApiException {
        destroy();
    }

    @Override
    protected void init() throws ApiException {
        System.setProperty("kubernetes.client", "true");

        kubernetesService.createSecret(SIDECAR_SECRET_NAME, new HashMap<String, String>() {{

            String keyProp = System.getProperty("awssecretaccesskey");
            String idProp = System.getProperty("awsaccesskeyid");

            String keyEnv = System.getenv("AWS_SECRET_KEY");
            String idEnv = System.getenv("AWS_ACCESS_KEY_ID");

            String key = keyProp != null ? keyProp : keyEnv;
            String id = idProp != null ? idProp : idEnv;

            put("awssecretaccesskey", key);
            put("awsaccesskeyid", id);
        }});
    }

    @Override
    protected void destroy() throws ApiException {
        kubernetesService.deleteSecret("test-sidecar-secret");
        System.setProperty("kubernetes.client", "false");
    }

    @Override
    protected BackupOperationRequest getBackupOperationRequest() {
        final BackupOperationRequest backupOperationRequest = new BackupOperationRequest();
        backupOperationRequest.k8sSecretName = SIDECAR_SECRET_NAME;
        return backupOperationRequest;
    }

    @Override
    public S3TransferManagerFactory getTransferManagerFactory() {
        return transferManagerFactory;
    }

    @Test
    public void testInPlaceBackupRestore() throws Exception {
        inPlaceTest(inPlaceArguments(CASSANDRA_VERSION));
    }

    @Test
    public void testImportingBackupAndRestore() throws Exception {
        liveCassandraTest(importArguments(CASSANDRA_4_VERSION), CASSANDRA_4_VERSION);
    }

    @Test
    @Ignore
    public void testHardlinkingBackupAndRestore() throws Exception {
        liveCassandraTest(hardlinkingArguments(CASSANDRA_4_VERSION), CASSANDRA_4_VERSION);
    }
}
