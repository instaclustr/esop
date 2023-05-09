package com.instaclustr.esop.s3;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Provider;
import com.instaclustr.esop.impl.AbstractOperationRequest;
import com.instaclustr.esop.s3.v1.S3ModuleException;
import com.instaclustr.kubernetes.KubernetesHelper;
import com.instaclustr.kubernetes.KubernetesSecretsReader;
import com.instaclustr.kubernetes.SecretReader;
import io.kubernetes.client.apis.CoreV1Api;

import static com.google.common.base.Strings.isNullOrEmpty;
import static com.instaclustr.esop.s3.S3ConfigurationResolver.S3Configuration.AWS_ACCESS_KEY_ID_PROPERTY;
import static com.instaclustr.esop.s3.S3ConfigurationResolver.S3Configuration.AWS_ENABLE_PATH_STYLE_ACCESS_PROPERTY;
import static com.instaclustr.esop.s3.S3ConfigurationResolver.S3Configuration.AWS_ENDPOINT_PROPERTY;
import static com.instaclustr.esop.s3.S3ConfigurationResolver.S3Configuration.AWS_KMS_KEY_ID_PROPERTY;
import static com.instaclustr.esop.s3.S3ConfigurationResolver.S3Configuration.AWS_REGION_PROPERTY;
import static com.instaclustr.esop.s3.S3ConfigurationResolver.S3Configuration.AWS_SECRET_ACCESS_KEY_PROPERTY;
import static com.instaclustr.kubernetes.KubernetesHelper.isRunningAsClient;
import static java.lang.Boolean.parseBoolean;
import static java.lang.String.format;

public class S3ConfigurationResolver {
    private static final Logger logger = LoggerFactory.getLogger(S3ConfigurationResolver.class);

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

    public static boolean isRunningInKubernetes() {
        return KubernetesHelper.isRunningInKubernetes() || isRunningAsClient();
    }

    public S3Configuration resolveS3Configuration(final Provider<CoreV1Api> coreV1ApiProvider, final AbstractOperationRequest operationRequest) {
        if (isRunningInKubernetes()) {
            if (isNullOrEmpty(operationRequest.resolveKubernetesSecretName())) {
                logger.warn("Kubernetes secret name for resolving S3 credentials was not specified, going to resolve them from env. properties. "
                            + "If env. properties are not specified, credentials will be fetched from AWS instance itself.");
                return resolveS3ConfigurationFromEnvProperties();
            }
            return resolveS3ConfigurationFromK8S(coreV1ApiProvider, operationRequest);
        }
        else {
            return resolveS3ConfigurationFromEnvProperties();
        }
    }

    private S3Configuration resolveS3ConfigurationFromK8S(Provider<CoreV1Api> coreV1ApiProvider,
                                                          AbstractOperationRequest operationRequest) {

        String secretName = operationRequest.resolveKubernetesSecretName();

        try
        {
            String namespace = resolveKubernetesKeyspace(operationRequest);
            SecretReader secretReader = new SecretReader(coreV1ApiProvider);

            return secretReader.readIntoObject(namespace,
                                               secretName,
                                               secret -> {
                                                   final Map<String, byte[]> data = secret.getData();

                                                   final S3Configuration s3Configuration = new S3Configuration();

                                                   final byte[] awsendpoint = data.get("awsendpoint");
                                                   final byte[] awsregion = data.get("awsregion");
                                                   final byte[] awssecretaccesskey = data.get("awssecretaccesskey");
                                                   final byte[] awsaccesskeyid = data.get("awsaccesskeyid");
                                                   final byte[] awsKmsKeyId = data.get("awskmskeyid");

                                                   if (awsendpoint != null)
                                                       s3Configuration.awsEndpoint = new String(awsendpoint);

                                                   if (awsregion != null)
                                                       s3Configuration.awsRegion = new String(awsregion);

                                                   if (awsaccesskeyid != null)
                                                       s3Configuration.awsAccessKeyId = new String(awsaccesskeyid);
                                                   else
                                                       throw new S3ModuleException(format("Secret %s does not contain any entry with key 'awsaccesskeyid'.",
                                                                                          secret.getMetadata().getName()));

                                                   if (awssecretaccesskey != null)
                                                       s3Configuration.awsSecretKey = new String(awssecretaccesskey);
                                                   else
                                                       throw new S3ModuleException(format("Secret %s does not contain any entry with key 'awssecretaccesskey'.",
                                                                                          secret.getMetadata().getName()));

                                                   if (awsKmsKeyId != null)
                                                       s3Configuration.awsKmsKeyId = new String(awsaccesskeyid);

                                                   return s3Configuration;
                                               });
        }
        catch (final Exception ex) {
            throw new S3ModuleException("Unable to resolve S3 credentials for backup / restores from Kubernetes secret " + secretName, ex);
        }
    }

    public S3Configuration resolveS3ConfigurationFromEnvProperties() {
        final S3Configuration s3Configuration = new S3Configuration();

        s3Configuration.awsRegion = System.getenv(AWS_REGION_PROPERTY);
        s3Configuration.awsEndpoint = System.getenv(AWS_ENDPOINT_PROPERTY);
        s3Configuration.awsAccessKeyId = System.getenv(AWS_ACCESS_KEY_ID_PROPERTY);
        s3Configuration.awsSecretKey = System.getenv(AWS_SECRET_ACCESS_KEY_PROPERTY);
        String awsEnablePathStyleAccess = System.getenv(AWS_ENABLE_PATH_STYLE_ACCESS_PROPERTY);
        s3Configuration.awsPathStyleAccessEnabled = awsEnablePathStyleAccess != null ? parseBoolean(awsEnablePathStyleAccess) : null;
        s3Configuration.awsKmsKeyId = System.getenv(AWS_KMS_KEY_ID_PROPERTY);

        if (s3Configuration.awsKmsKeyId == null)
            s3Configuration.awsKmsKeyId = System.getProperty(AWS_KMS_KEY_ID_PROPERTY);

        // accesskeyid and awssecretkey will be taken from normal configuration mechanism in ~/.aws/ ...
        // s3Configuration.awsAccessKeyId = System.getenv("AWS_ACCESS_KEY_ID");
        // s3Configuration.awsSecretKey = System.getenv("AWS_SECRET_KEY");

        return s3Configuration;
    }

    private String resolveKubernetesKeyspace(final AbstractOperationRequest operationRequest) {
        if (operationRequest.resolveKubernetesNamespace() != null)
            return operationRequest.resolveKubernetesNamespace();
        else
            return KubernetesSecretsReader.readNamespace();
    }
}
