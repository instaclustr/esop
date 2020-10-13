package com.instaclustr.esop.backup.embedded.gcp;

import static org.testng.Assert.assertNotNull;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;

import com.google.inject.Inject;
import com.instaclustr.esop.gcp.GCPModule.GoogleStorageFactory;
import com.instaclustr.esop.impl.backup.BackupOperationRequest;
import com.instaclustr.kubernetes.KubernetesService;
import io.kubernetes.client.ApiException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Ignore;
import org.testng.annotations.Test;

@Test(groups = {
    "k8sTest",
    "googleTest",
})
@Ignore
public class KubernetesGoogleStorageBackupRestoreTest extends BaseGoogleStorageBackupRestoreTest {

    @Inject
    public KubernetesService kubernetesService;

    @Inject
    public GoogleStorageFactory googleStorageFactory;

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
    protected void init() throws ApiException, IOException {
        System.setProperty("kubernetes.client", "true");

        assertNotNull(kubernetesService);

        kubernetesService.createSecret(SIDECAR_SECRET_NAME, new HashMap<String, String>() {{

            String gacProperty = System.getProperty("google.application.credentials");

            String gacEnvProperty = System.getenv("GOOGLE_APPLICATION_CREDENTIALS");

            String creds = gacProperty != null ? gacProperty : gacEnvProperty;

            put("gcp", new String(Files.readAllBytes(Paths.get(creds))));
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
    public GoogleStorageFactory getGoogleStorageFactory() {
        return googleStorageFactory;
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
