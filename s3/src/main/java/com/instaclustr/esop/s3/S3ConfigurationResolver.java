package com.instaclustr.esop.s3;

import com.instaclustr.esop.impl.AbstractOperationRequest;

import static com.instaclustr.esop.s3.S3ConfigurationResolver.S3Configuration.AWS_ACCESS_KEY_ID_PROPERTY;
import static com.instaclustr.esop.s3.S3ConfigurationResolver.S3Configuration.AWS_ENABLE_PATH_STYLE_ACCESS_PROPERTY;
import static com.instaclustr.esop.s3.S3ConfigurationResolver.S3Configuration.AWS_ENDPOINT_PROPERTY;
import static com.instaclustr.esop.s3.S3ConfigurationResolver.S3Configuration.AWS_KMS_KEY_ID_PROPERTY;
import static com.instaclustr.esop.s3.S3ConfigurationResolver.S3Configuration.AWS_REGION_PROPERTY;
import static com.instaclustr.esop.s3.S3ConfigurationResolver.S3Configuration.AWS_SECRET_ACCESS_KEY_PROPERTY;
import static java.lang.Boolean.parseBoolean;

public class S3ConfigurationResolver {

    public AbstractOperationRequest request;
    public String kmsKeyIdFromRequest;

    public S3ConfigurationResolver() {
    }

    public S3ConfigurationResolver(AbstractOperationRequest request) {
        this.request = request;
        this.kmsKeyIdFromRequest = request.kmsKeyId;
    }

    public static final class S3Configuration {
        public static final String TEST_ESOP_AWS_KMS_WRAPPING_KEY = "TEST_ESOP_AWS_KMS_WRAPPING_KEY";
        public static final String AWS_REGION_PROPERTY = "AWS_REGION";
        public static final String AWS_ENDPOINT_PROPERTY = "AWS_ENDPOINT";
        public static final String AWS_ACCESS_KEY_ID_PROPERTY = "AWS_ACCESS_KEY_ID";
        public static final String AWS_SECRET_ACCESS_KEY_PROPERTY = "AWS_SECRET_ACCESS_KEY";
        public static final String AWS_ENABLE_PATH_STYLE_ACCESS_PROPERTY = "AWS_ENABLE_PATH_STYLE_ACCESS";
        public static final String AWS_KMS_KEY_ID_PROPERTY = "AWS_KMS_KEY_ID";

        public String awsRegion;
        public String awsEndpoint;
        public String awsAccessKeyId;
        public String awsSecretKey;
        public Boolean awsPathStyleAccessEnabled;
        public String awsKmsKeyId;
    }

    public S3Configuration resolveS3ConfigurationFromEnvProperties() {
        final S3Configuration s3Configuration = new S3Configuration();

        s3Configuration.awsRegion = System.getenv(AWS_REGION_PROPERTY);
        s3Configuration.awsEndpoint = System.getenv(AWS_ENDPOINT_PROPERTY);
        s3Configuration.awsAccessKeyId = System.getenv(AWS_ACCESS_KEY_ID_PROPERTY);
        s3Configuration.awsSecretKey = System.getenv(AWS_SECRET_ACCESS_KEY_PROPERTY);
        String awsEnablePathStyleAccess = System.getenv(AWS_ENABLE_PATH_STYLE_ACCESS_PROPERTY);
        s3Configuration.awsPathStyleAccessEnabled = awsEnablePathStyleAccess != null ? parseBoolean(awsEnablePathStyleAccess) : null;

        s3Configuration.awsKmsKeyId = kmsKeyIdFromRequest;

        if (s3Configuration.awsKmsKeyId == null)
            s3Configuration.awsKmsKeyId = System.getProperty(AWS_KMS_KEY_ID_PROPERTY);

        if (s3Configuration.awsKmsKeyId == null)
            s3Configuration.awsKmsKeyId = System.getenv(AWS_KMS_KEY_ID_PROPERTY);

        // accesskeyid and awssecretkey will be taken from normal configuration mechanism in ~/.aws/ ...
        // s3Configuration.awsAccessKeyId = System.getenv("AWS_ACCESS_KEY_ID");
        // s3Configuration.awsSecretKey = System.getenv("AWS_SECRET_KEY");

        return s3Configuration;
    }
}
