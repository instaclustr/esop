package com.instaclustr.esop.backup.embedded.s3.aws.v2;

import java.nio.file.Paths;

import org.testng.Assert;
import org.testng.annotations.Ignore;
import org.testng.annotations.Test;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.profiles.ProfileFile;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.kms.model.CreateKeyRequest;
import software.amazon.awssdk.services.kms.model.CreateKeyResponse;
import software.amazon.awssdk.services.kms.model.CustomerMasterKeySpec;

public class AmazonKMSTest
{
    @Test
    public void testKMS()
    {
        AwsCredentials awsCredentials = DefaultCredentialsProvider.create().resolveCredentials();

        KmsClient client = KmsClient.builder()
                                    .credentialsProvider(DefaultCredentialsProvider.create())
                                    .region(Region.US_EAST_1).build();

        CreateKeyRequest keyRequest = CreateKeyRequest.builder()
                                                      .description("instaclustr-stefan-test-key-3")
                                                      .customerMasterKeySpec(CustomerMasterKeySpec.SYMMETRIC_DEFAULT.SYMMETRIC_DEFAULT)
                                                      .build();
        Assert.assertNotNull(client);

        CreateKeyResponse key = client.createKey(keyRequest);
        String keyId = key.keyMetadata().keyId();

        System.out.println(keyId);
    }
}
