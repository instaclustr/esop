package com.instaclustr.picocli.typeconverter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.instaclustr.measure.Time;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TimeTypeConverterTest {

    private final TimeMeasureTypeConverter converter = new TimeMeasureTypeConverter();

    @Test
    public void testTimeConversion() throws Exception {
        Time minutes = converter.convert("1m");
        Assertions.assertEquals(Long.valueOf(1), minutes.value);
        Assertions.assertEquals(Time.TimeUnit.MINUTES, minutes.unit);

        Time seconds = converter.convert("1s");
        Assertions.assertEquals(Long.valueOf(1), seconds.value);
        Assertions.assertEquals(Time.TimeUnit.SECONDS, seconds.unit);

        Time hours = converter.convert("1h");
        Assertions.assertEquals(Long.valueOf(1), hours.value);
        Assertions.assertEquals(Time.TimeUnit.HOURS, hours.unit);

        Time days = converter.convert("1d");
        Assertions.assertEquals(Long.valueOf(1), days.value);
        Assertions.assertEquals(Time.TimeUnit.DAYS, days.unit);
    }

    @Test
    public void testTimeJsonConversion() throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();

        Time minute = converter.convert("1m");

        String minuteInJson = objectMapper.writer().writeValueAsString(minute);

        Time deserializedMinute = objectMapper.readValue(minuteInJson, Time.class);

        Assertions.assertEquals(minute, deserializedMinute);
    }
}
