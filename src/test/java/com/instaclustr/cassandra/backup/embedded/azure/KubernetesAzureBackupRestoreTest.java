package com.instaclustr.cassandra.backup.embedded.azure;

import static org.testng.Assert.assertNotNull;

import java.util.HashMap;

import com.google.inject.Inject;
import com.instaclustr.cassandra.backup.azure.AzureModule.CloudStorageAccountFactory;
import com.instaclustr.cassandra.backup.impl.backup.BackupOperationRequest;
import com.instaclustr.kubernetes.KubernetesService;
import io.kubernetes.client.ApiException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Ignore;
import org.testng.annotations.Test;

@Test(groups = {
    "k8sTest",
    "azureTest",
})
public class KubernetesAzureBackupRestoreTest extends BaseAzureBackupRestoreTest {

    @Inject
    public KubernetesService kubernetesService;

    @Inject
    public CloudStorageAccountFactory cloudStorageAccountFactory;

    @BeforeMethod
    public void setup() throws Exception {
        inject();
        init();
    }

    @AfterMethod
    public void teardown() throws Exception {
        destroy();
    }

    @Override
    protected void init() throws ApiException {
        System.setProperty("kubernetes.client", "true");

        assertNotNull(kubernetesService);

        kubernetesService.createSecret(SIDECAR_SECRET_NAME, new HashMap<String, String>() {{

            String accountProp = System.getProperty("azurestorageaccount");
            String keyProp = System.getProperty("azurestoragekey");

            String accountEnv = System.getenv("AZURE_STORAGE_ACCOUNT");
            String keyEnv = System.getenv("AZURE_STORAGE_KEY");

            String account = accountProp != null ? accountProp : accountEnv;
            String key = keyProp != null ? keyProp : keyEnv;

            put("azurestorageaccount", account);
            put("azurestoragekey", key);
        }});
    }

    @Override
    protected void destroy() throws ApiException {
        kubernetesService.deleteSecret(SIDECAR_SECRET_NAME);
        System.setProperty("kubernetes.client", "false");
    }

    @Override
    protected BackupOperationRequest getBackupOperationRequest() {
        final BackupOperationRequest backupOperationRequest = new BackupOperationRequest();
        backupOperationRequest.k8sSecretName = SIDECAR_SECRET_NAME;
        return backupOperationRequest;
    }

    @Override
    public CloudStorageAccountFactory getStorageAccountFactory() {
        return cloudStorageAccountFactory;
    }

    @Test
    public void testInPlaceBackupRestore() throws Exception {
        inPlaceTest(inPlaceArguments());
    }

    @Test
    public void testImportingBackupAndRestore() throws Exception {
        liveCassandraTest(importArguments(), CASSANDRA_4_VERSION);
    }

    @Test
    @Ignore
    public void testHardlinkingBackupAndRestore() throws Exception {
        liveCassandraTest(hardlinkingArguments(), CASSANDRA_VERSION);
    }
}
