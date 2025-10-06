package com.instaclustr.measure;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class Time extends Measure<Long, Time.TimeUnit> {

    @JsonCreator
    public Time(@JsonProperty("value") final Long value,
                @JsonProperty("unit") final TimeUnit unit) {
        super(value, unit);
    }

    public Time asSeconds() {
        return new Time(unit.toMilliseconds(value) / 1000, TimeUnit.SECONDS);
    }

    public long toMilliseconds() {
        return asSeconds().value * 1000;
    }

    public static Time zeroTime() {
        return new Time(0L, TimeUnit.SECONDS);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (!(obj instanceof Time))
            return false;

        final Time other = (Time) obj;

        return this.value.equals(other.value) && this.unit == other.unit;
    }

    public enum TimeUnit {
        SECONDS("s", "seconds") {
            @Override
            long toMilliseconds(final long value) {
                return value * 1000;
            }
        },
        MINUTES("m", "minutes") {
            @Override
            long toMilliseconds(final long value) {
                return value * 60 * 1000;
            }
        },
        HOURS("h", "hours") {
            @Override
            long toMilliseconds(final long value) {
                return value * 60 * 60 * 1000;
            }
        },
        DAYS("d", "days") {
            @Override
            long toMilliseconds(final long value) {
                return value * 24 * 60 * 60 * 1000;
            }
        },;

        final String unit, description;

        @JsonCreator
        TimeUnit(@JsonProperty("unit") final String unit, @JsonProperty("description") final String description) {
            this.unit = unit;
            this.description = description;
        }

        abstract long toMilliseconds(long value);

        @Override
        public String toString() {
            return unit;
        }
    }
}
