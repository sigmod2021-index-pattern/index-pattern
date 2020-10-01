package sigmod2021.esp.expressions.bool;

import sigmod2021.esp.bindings.BoundVariables;
import sigmod2021.event.Event;

import java.util.List;

public class EVAnd implements EVBooleanExpression {

    private final EVBooleanExpression left;

    private final EVBooleanExpression right;

    public EVAnd(EVBooleanExpression left, EVBooleanExpression right) {
        this.left = left;
        this.right = right;
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public Boolean eval(Event event, BoundVariables bindings) {
        return left.eval(event, bindings) && right.eval(event, bindings);
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public Boolean eval(List<Event> previousEvents, Event event, BoundVariables bindings) {
        return left.eval(previousEvents, event, bindings) && right.eval(previousEvents, event, bindings);
    }
}
