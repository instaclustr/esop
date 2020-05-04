package com.instaclustr.cassandra.backup.impl.restore;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.instaclustr.operations.Operation;
import picocli.CommandLine;

/**
 * Strategy telling how restore should be carried out. Strategy is in general composed of phases.
 * All phases might be done in sequence in one execution or a particular phase may run only in
 * case some depending phase has finished (across whole cluster, per node).
 */
public interface RestorationStrategy {

    /**
     * Executes a restoration of a node or just its phase.
     *
     * @param restorer restorer to use upon restoration, the most probable usage is downloading of SSTables from remote locations
     * @param operation operation this strategy is executed on
     * @throws Exception in case restoration has failed
     * @see RestorationPhase
     */
    void restore(Restorer restorer, Operation<RestoreOperationRequest> operation) throws Exception;

    default void isEligibleToRun() throws Exception {

    }

    RestorationStrategyType getStrategyType();

    enum RestorationStrategyType {
        IN_PLACE, // only against stopped node
        HARDLINKS, // only on running node
        IMPORT, // only on running node and against Cassandra 4
        UNKNOWN;

        @JsonCreator
        public static RestorationStrategyType forValue(String value) {

            if (value == null || value.isEmpty()) {
                return RestorationStrategyType.UNKNOWN;
            }

            try {
                return RestorationStrategyType.valueOf(value.trim().toUpperCase());
            } catch (final IllegalArgumentException ex) {
                return RestorationStrategyType.UNKNOWN;
            }
        }

        @JsonValue
        public String toValue() {
            return this.toString();
        }
    }

    class RestorationStrategyTypeConverter implements CommandLine.ITypeConverter<RestorationStrategyType> {

        @Override
        public RestorationStrategyType convert(final String value) {
            return RestorationStrategyType.forValue(value);
        }
    }
}
