package sigmod2021.esp.bridges.nat.epa.impl.aggregator.aggregates;

import java.io.Serializable;

/**
 * Abstracts (partial) aggregates.
 */
public interface AggregateFunction<T_MERGE, T_EVAL, T_INPUT> extends Serializable {

    /**
     * @return the neutral value of this aggregate
     */
    T_MERGE fInit();

    /**
     * Creates a partial aggregate from the given input-value
     *
     * @param initialValue the input-value
     * @return a partial-aggregate from the given input-value
     */
    T_MERGE fInit(T_INPUT initialValue);

    /**
     * Merges two partial aggregates
     *
     * @param left  the left partial aggregate
     * @param right the right partial aggregate
     * @return the merged partial aggregate
     */
    T_MERGE fMerge(T_MERGE left, T_MERGE right);

    /**
     * Evaluates the given partial aggregate
     *
     * @param value   the partial aggregate value
     * @param counter the number of values the given aggregate represents
     * @return the result-value for the given partial aggregate and counter
     */
    T_EVAL fEval(T_MERGE value, long counter);

}
