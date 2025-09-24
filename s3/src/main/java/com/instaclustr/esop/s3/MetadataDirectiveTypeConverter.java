package com.instaclustr.esop.s3;

import picocli.CommandLine;
import software.amazon.awssdk.services.s3.model.MetadataDirective;

public class MetadataDirectiveTypeConverter implements CommandLine.ITypeConverter<MetadataDirective>
{
    @Override
    public MetadataDirective convert(final String value)
    {
        return MetadataDirective.fromValue(value.toUpperCase());
    }
}
