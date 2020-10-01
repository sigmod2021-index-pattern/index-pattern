package sigmod2021.esp.expressions.util;

import sigmod2021.esp.bindings.BoundVariables;
import sigmod2021.esp.expressions.EvaluableExpression;
import sigmod2021.event.Event;

import java.util.List;

public abstract class EVAbstractTrinaryExpression<TIN1, TIN2, TIN3, TOUT> implements EvaluableExpression<TOUT> {

    private final EvaluableExpression<TIN1> in1;
    private final EvaluableExpression<TIN2> in2;
    private final EvaluableExpression<TIN3> in3;
    private final OperatorImpl<TIN1, TIN2, TIN3, TOUT> impl;
    public EVAbstractTrinaryExpression(EvaluableExpression<TIN1> in1, EvaluableExpression<TIN2> in2,
                                       EvaluableExpression<TIN3> in3, OperatorImpl<TIN1, TIN2, TIN3, TOUT> impl) {
        this.in1 = in1;
        this.in2 = in2;
        this.in3 = in3;
        this.impl = impl;
    }

    @Override
    public TOUT eval(Event event, BoundVariables bindings) {
        return impl.apply(in1.eval(event, bindings), in2.eval(event, bindings), in3.eval(event, bindings));
    }

    @Override
    public TOUT eval(List<Event> previousEvents, Event event, BoundVariables bindings) {
        return impl.apply(in1.eval(previousEvents, event, bindings),
                in2.eval(previousEvents, event, bindings),
                in3.eval(previousEvents, event, bindings));
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((impl == null) ? 0 : impl.hashCode());
        result = prime * result + ((in1 == null) ? 0 : in1.hashCode());
        result = prime * result + ((in2 == null) ? 0 : in2.hashCode());
        result = prime * result + ((in3 == null) ? 0 : in3.hashCode());
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
        EVAbstractTrinaryExpression<?, ?, ?, ?> other = (EVAbstractTrinaryExpression<?, ?, ?, ?>) obj;
        if (impl == null) {
            if (other.impl != null)
                return false;
        } else if (!impl.equals(other.impl))
            return false;
        if (in1 == null) {
            if (other.in1 != null)
                return false;
        } else if (!in1.equals(other.in1))
            return false;
        if (in2 == null) {
            if (other.in2 != null)
                return false;
        } else if (!in2.equals(other.in2))
            return false;
        if (in3 == null) {
            if (other.in3 != null)
                return false;
        } else if (!in3.equals(other.in3))
            return false;
        return true;
    }

    protected interface OperatorImpl<TIN1, TIN2, TIN3, TOUT> {
        TOUT apply(TIN1 in1, TIN2 in2, TIN3 in3);
    }
}
