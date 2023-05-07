package com.instaclustr.esop.impl.retry;

import java.util.Arrays;

import com.google.common.base.MoreObjects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;
import picocli.CommandLine;
import picocli.CommandLine.Option;

import static java.lang.String.format;

public class RetrySpec {

    @Option(names = "--retry-interval",
        defaultValue = "10",
        description = "interval between retries when downloading of SSTable file fails, in seconds, defalts to 10")
    public int interval;

    @Option(names = "--retry-strategy",
        defaultValue = "linear",
        description = "strategy to use for retries, either 'linear' or 'exponential', defaults to 'linear'",
        converter = RetryStrategyConverter.class)
    public RetryStrategy strategy;

    @Option(names = "--retry-max-attempts",
        defaultValue = "3",
        description = "number of attempts to download SSTable file, defaults to 3")
    public int maxAttempts;

    @Option(names = "--retry-enabled",
        description = "flag telling if retry mechanism is enabled or not, defaults to false")
    public boolean enabled;

    @JsonCreator
    public RetrySpec(@JsonProperty("interval") final Integer interval,
                     @JsonProperty("strategy") final RetryStrategy strategy,
                     @JsonProperty("maxAttempts") final Integer maxAttempts,
                     @JsonProperty("enabled") final boolean enabled) {
        this.interval = interval == null || interval < 1 ? 10 : interval;
        this.strategy = strategy == null ? RetryStrategy.LINEAR : strategy;
        this.enabled = enabled;
        this.maxAttempts = maxAttempts == null || maxAttempts < 1 ? 3 : maxAttempts;
    }

    public RetrySpec() {
        this.interval = 10;
        this.strategy = RetryStrategy.LINEAR;
        this.maxAttempts = 3;
        this.enabled = false;
    }

    public void validate() {
        if (strategy == null) {
            strategy = RetryStrategy.LINEAR;
        }
        if (interval < 1) {
            interval = 10;
        }
        if (maxAttempts < 1) {
            maxAttempts = 3;
        }
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("interval", interval)
            .add("strategy", strategy)
            .add("maxAttempts", maxAttempts)
            .add("enabled", enabled)
            .toString();
    }

    private static class RetryStrategyConverter implements CommandLine.ITypeConverter<RetryStrategy> {

        @Override
        public RetryStrategy convert(final String value) {
            return RetryStrategy.parse(value);
        }
    }

    public enum RetryStrategy {
        EXPONENTIAL,
        LINEAR;

        private static final Logger logger = LoggerFactory.getLogger(RetryStrategy.class);
        public static final RetryStrategy DEFAULT_STRATEGY = LINEAR;

        @JsonCreator
        public static RetryStrategy parse(final String value) {
            if (value == null || value.trim().isEmpty()) {
                return RetryStrategy.DEFAULT_STRATEGY;
            }

            for (final RetryStrategy strategy : RetryStrategy.values()) {
                if (strategy.name().toLowerCase().equals(value.toLowerCase())) {
                    return strategy;
                }
            }

            logger.info(format("Unable to parse retry strategy for value '%s', possible strategies: %s, returning default algorithm %s",
                               value,
                               Arrays.toString(RetryStrategy.values()),
                               RetryStrategy.DEFAULT_STRATEGY));

            return RetryStrategy.DEFAULT_STRATEGY;
        }

        @JsonValue
        public String toValue() {
            return this.toString();
        }
    }
}
