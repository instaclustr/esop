package com.instaclustr.esop.impl.restore;

import com.instaclustr.esop.impl.AbstractOperationRequest;
import com.instaclustr.esop.impl.ProxySettings;
import com.instaclustr.esop.impl.StorageLocation;
import com.instaclustr.esop.impl.retry.RetrySpec;

public class BaseRestoreOperationRequest extends AbstractOperationRequest {

    public BaseRestoreOperationRequest() {
        // for picocli
    }

    public BaseRestoreOperationRequest(final StorageLocation storageLocation,
                                       final Integer concurrentConnections,
                                       final String k8sNamespace,
                                       final String k8sSecretName,
                                       final boolean insecure,
                                       final boolean skipBucketVerification,
                                       final ProxySettings proxySettings,
                                       final RetrySpec retry) {
        super(storageLocation, k8sNamespace, k8sSecretName, insecure, skipBucketVerification, proxySettings, retry, concurrentConnections);
    }
}
