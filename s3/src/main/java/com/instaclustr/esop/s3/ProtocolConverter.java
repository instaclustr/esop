package com.instaclustr.esop.s3;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import software.amazon.awssdk.services.s3.model.Protocol;

public final class ProtocolConverter implements CommandLine.ITypeConverter<Protocol>
{

    private static final Logger logger = LoggerFactory.getLogger(ProtocolConverter.class);

    @Override
    public Protocol convert(final String value)
    {
        if (value == null)
        {
            return Protocol.HTTPS;
        }

        try
        {
            return Protocol.valueOf(value.toLowerCase());
        } catch (final Exception ex)
        {
            logger.warn(String.format("Unable to parse protocol of value '%s', using %s", value, Protocol.HTTPS));
            return Protocol.HTTPS;
        }
    }
}
