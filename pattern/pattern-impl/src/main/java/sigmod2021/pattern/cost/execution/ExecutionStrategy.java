package sigmod2021.pattern.cost.execution;

import sigmod2021.common.IncompatibleTypeException;
import sigmod2021.pattern.cost.transform.TransformedPattern;
import sigmod2021.esp.ql.TranslatorException;
import sigmod2021.event.Event;
import xxl.core.cursors.Cursor;

/**
 *
 */
public interface ExecutionStrategy extends Comparable<ExecutionStrategy> {


    Estimates getEstimates();

    ExecutionStrategy cloneWithNewEstimates(Estimates estimates);

    /**
     * @{inheritDoc}
     */
    @Override
    default int compareTo(ExecutionStrategy o) {
        return getEstimates().compareTo(o.getEstimates());
    }

    Cursor<Event> execute() throws TranslatorException, IncompatibleTypeException;

    long executeIOOnly();

    default boolean dominates(ExecutionStrategy o) {
        return getEstimates().dominates(o.getEstimates());
    }

    default boolean isEquivalent(TransformedPattern.ExecutableConfiguration config) {
        return getConfig().equals(config);
    }

    ;

    default boolean isEquivalent(ExecutionStrategy s) {
        return isEquivalent(s.getConfig());
    }

    TransformedPattern.ExecutableConfiguration getConfig();

}
