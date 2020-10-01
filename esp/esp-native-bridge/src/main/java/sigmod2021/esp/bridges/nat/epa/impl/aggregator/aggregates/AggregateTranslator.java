package sigmod2021.esp.bridges.nat.epa.impl.aggregator.aggregates;

import sigmod2021.esp.api.epa.aggregates.*;
import sigmod2021.esp.api.epa.aggregates.spatial.TemporalLineStringMerge;
import sigmod2021.esp.api.epa.aggregates.spatial.Trajectory;
import sigmod2021.esp.api.epa.pattern.symbol.NoSuchVariableException;
import sigmod2021.esp.bridges.nat.epa.impl.aggregator.aggregates.first.FirstFunction;
import sigmod2021.esp.bridges.nat.epa.impl.aggregator.aggregates.last.LastFunction;
import sigmod2021.esp.bridges.nat.epa.impl.aggregator.aggregates.max.*;
import sigmod2021.esp.bridges.nat.epa.impl.aggregator.aggregates.min.*;
import sigmod2021.esp.bridges.nat.epa.impl.aggregator.aggregates.sum.DoubleSumFunction;
import sigmod2021.esp.bridges.nat.epa.impl.aggregator.aggregates.sum.LongSumFunction;
import sigmod2021.event.Attribute;
import sigmod2021.event.EventSchema;
import sigmod2021.event.SchemaException;

/**
 * Translates Mock-Up aggregates into {@link AggregateFunction functions}.
 */
public final class AggregateTranslator {

    private AggregateTranslator() {
    }

    /**
     * Creates a partial aggregate function from the given aggregate
     *
     * @param agg         the aggregate mock-up
     * @param inputSchema the schema of incoming events
     * @return partial aggregate function for the given mock-up
     * @throws NoSuchVariableException
     */
    @SuppressWarnings("rawtypes")
    public static AggregateFunction<?, ?, ?> translateAggregate(Aggregate agg, EventSchema inputSchema) throws SchemaException {
        if (agg instanceof Average)
            return new AverageFunction();
        else if (agg instanceof Count)
            return new CountFunction();
        else if (agg instanceof Stddev)
            return new StdDevFunction();
        else if (agg instanceof Sum)
            return createSum((Sum) agg, inputSchema.byName(agg.getAttributeIn()));
        else if (agg instanceof Trajectory)
            return new TrajectoryFunction();
        else if (agg instanceof Last)
            return new LastFunction();
        else if (agg instanceof First)
            return new FirstFunction();
        else if (agg instanceof Minimum)
            return createMin((Minimum) agg, inputSchema.byName(agg.getAttributeIn()));
        else if (agg instanceof Maximum)
            return createMax((Maximum) agg, inputSchema.byName(agg.getAttributeIn()));
        else if (agg instanceof TemporalLineStringMerge) {
            return new TemporalLineStringMergeFunction();
        } else
            throw new IllegalArgumentException("Unknown aggregate: " + agg.getClass().getSimpleName());
    }

    /**
     * Creates a sum aggregate
     *
     * @param s the mock-up
     * @param a the attribute the sum is calculated on
     * @return the sum aggregate function
     */
    private static AggregateFunction<?, ?, ?> createSum(Sum s, Attribute a) {
        switch (a.getType()) {
            case BYTE:
            case SHORT:
            case INTEGER:
            case LONG:
                return new LongSumFunction();
            case FLOAT:
            case DOUBLE:
                return new DoubleSumFunction();
            default:
                throw new IllegalArgumentException("Cannot sum attribute: " + s.getAttributeIn() + ":" + a.getType());
        }
    }

    /**
     * Creates a minimum aggregate
     *
     * @param s the mock-up
     * @param a the attribute the minimum is calculated on
     * @return the minimum aggregate function
     */
    private static AggregateFunction<?, ?, ?> createMin(Minimum s, Attribute a) {
        switch (a.getType()) {
            case BYTE:
                return new ByteMinFunction();
            case SHORT:
                return new ShortMinFunction();
            case INTEGER:
                return new IntMinFunction();
            case LONG:
                return new LongMinFunction();
            case FLOAT:
                return new FloatMinFunction();
            case DOUBLE:
                return new DoubleMinFunction();
            default:
                throw new IllegalArgumentException("Cannot calc minimum on attribute: " + s.getAttributeIn() + ":"
                        + a.getType());
        }
    }

    /**
     * Creates a maximum aggregate
     *
     * @param s the mock-up
     * @param a the attribute the maximum is calculated on
     * @return the maximum aggregate function
     */
    private static AggregateFunction<?, ?, ?> createMax(Maximum s, Attribute a) {
        switch (a.getType()) {
            case BYTE:
                return new ByteMaxFunction();
            case SHORT:
                return new ShortMaxFunction();
            case INTEGER:
                return new IntMaxFunction();
            case LONG:
                return new LongMaxFunction();
            case FLOAT:
                return new FloatMaxFunction();
            case DOUBLE:
                return new DoubleMaxFunction();
            default:
                throw new IllegalArgumentException("Cannot calc maximum on attribute: " + s.getAttributeIn() + ":"
                        + a.getType());
        }
    }

}
