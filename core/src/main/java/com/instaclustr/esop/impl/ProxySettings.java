package com.instaclustr.esop.impl;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import picocli.CommandLine.Option;

public class ProxySettings {

    @Option(names = "--use-proxy", description = "if specified, it will use proxy client configuration relevant for S3 only.")
    public boolean useProxy;

    @Option(names = "--proxy-host", description = "host for proxy, relevant for S3 only")
    public String proxyHost;

    @Option(names = "--proxy-port", description = "port for proxy, relevant for S3 only")
    public Integer proxyPort;

    @Option(names = "--proxy-username", description = "username for proxy, relevant for S3 only")
    public String proxyUsername;

    @Option(names = "--proxy-password", description = "password for proxy, relevant for S3 only")
    public String proxyPassword;

    @Option(names = "--proxy-protocol", description = "protocol for proxy, defaults to HTTPS, might be HTTP or HTTPS")
    public String proxyProtocol;

    public ProxySettings() {
        // for picocli
    }

    @JsonCreator
    public ProxySettings(@JsonProperty("useProxy") final boolean useProxy,
                         @JsonProperty("proxyHost") final String proxyHost,
                         @JsonProperty("proxyPort") final Integer proxyPort,
                         @JsonProperty("proxyUsername") final String proxyUsername,
                         @JsonProperty("proxyPassword") final String proxyPassword,
                         @JsonProperty("proxyProtocol") final String proxyProtocol) {
        this.useProxy = useProxy;
        this.proxyHost = proxyHost;
        this.proxyPort = proxyPort;
        this.proxyUsername = proxyUsername;
        this.proxyPassword = proxyPassword;
        this.proxyProtocol = proxyProtocol;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("useProxy", useProxy)
            .add("proxyHost", proxyHost)
            .add("proxyPort", proxyPort)
            .add("proxyUsername", proxyUsername)
            .add("proxyPassword", "redacted")
            .add("proxyProtocol", proxyProtocol)
            .toString();
    }

}
