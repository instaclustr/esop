package com.instaclustr.cassandra.backup.impl;

import static com.amazonaws.Protocol.HTTPS;

import java.io.IOException;

import com.amazonaws.Protocol;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.exc.InvalidFormatException;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.google.common.base.MoreObjects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.ITypeConverter;
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

    @Option(names = "--proxy-protocol",
        converter = ProtocolConverter.class, description = "protocol for proxy, defaults to HTTPS, might be HTTP or HTTPS")
    @JsonSerialize(using = ProtocolSerializer.class)
    @JsonDeserialize(using = ProtocolDeserializer.class)
    public Protocol proxyProtocol;

    public ProxySettings() {
        // for picocli
    }

    @JsonCreator
    public ProxySettings(@JsonProperty("useProxy") final boolean useProxy,
                         @JsonProperty("proxyHost") final String proxyHost,
                         @JsonProperty("proxyPort") final Integer proxyPort,
                         @JsonProperty("proxyUsername") final String proxyUsername,
                         @JsonProperty("proxyPassword") final String proxyPassword,
                         @JsonProperty("proxyProtocol")
                         @JsonSerialize(using = ProtocolSerializer.class)
                         @JsonDeserialize(using = ProtocolDeserializer.class) final Protocol proxyProtocol) {
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

    public static final class ProtocolConverter implements ITypeConverter<Protocol> {

        private static final Logger logger = LoggerFactory.getLogger(ProtocolConverter.class);

        @Override
        public Protocol convert(final String value) {
            if (value == null) {
                return HTTPS;
            }

            try {
                return Protocol.valueOf(value.toLowerCase());
            } catch (final Exception ex) {
                logger.warn(String.format("Unable to parse protocol of value '%s', using %s", value, HTTPS));
                return HTTPS;
            }
        }
    }

    public static class ProtocolSerializer extends StdSerializer<Protocol> {

        public ProtocolSerializer() {
            super(Protocol.class);
        }

        protected ProtocolSerializer(final Class<Protocol> t) {
            super(t);
        }

        @Override
        public void serialize(final Protocol value, final JsonGenerator gen, final SerializerProvider provider) throws IOException {
            if (value != null) {
                gen.writeString(value.toString().toLowerCase());
            }
        }
    }

    public static class ProtocolDeserializer extends StdDeserializer<Protocol> {

        public ProtocolDeserializer() {
            super(Protocol.class);
        }

        protected ProtocolDeserializer(final Class<?> vc) {
            super(vc);
        }

        @Override
        public Protocol deserialize(final JsonParser p, final DeserializationContext ctxt) throws IOException, JsonProcessingException {
            final String valueAsString = p.getValueAsString();

            if (valueAsString == null) {
                return null;
            }

            try {
                return Protocol.valueOf(valueAsString.toUpperCase());
            } catch (final Exception ex) {
                throw new InvalidFormatException(p, "Invalid Protocol", valueAsString, Protocol.class);
            }
        }
    }
}
