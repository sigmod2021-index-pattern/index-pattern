package sigmod2021.esp.expressions.util;

import sigmod2021.esp.bindings.BoundVariables;
import sigmod2021.esp.expressions.EvaluableExpression;
import sigmod2021.event.Event;

import java.util.List;

public abstract class EVAbstractUnaryExpression<TIN, TOUT> implements EvaluableExpression<TOUT> {

    private final EvaluableExpression<TIN> input;
    private final OperatorImpl<TIN, TOUT> impl;
    public EVAbstractUnaryExpression(EvaluableExpression<TIN> input, OperatorImpl<TIN, TOUT> impl) {
        this.input = input;
        this.impl = impl;
    }

    @Override
    public TOUT eval(Event event, BoundVariables bindings) {
        return impl.apply(input.eval(event, bindings));
    }

    @Override
    public TOUT eval(List<Event> previousEvents, Event event, BoundVariables bindings) {
        return impl.apply(input.eval(previousEvents, event, bindings));
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((impl == null) ? 0 : impl.hashCode());
        result = prime * result + ((input == null) ? 0 : input.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        EVAbstractUnaryExpression<?, ?> other = (EVAbstractUnaryExpression<?, ?>) obj;
        if (impl == null) {
            if (other.impl != null)
                return false;
        } else if (!impl.equals(other.impl))
            return false;
        if (input == null) {
            if (other.input != null)
                return false;
        } else if (!input.equals(other.input))
            return false;
        return true;
    }

    protected static interface OperatorImpl<TIN, TOUT> {
        TOUT apply(TIN in);
    }
}
