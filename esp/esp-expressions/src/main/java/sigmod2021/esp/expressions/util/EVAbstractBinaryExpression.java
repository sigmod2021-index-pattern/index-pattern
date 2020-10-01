package sigmod2021.esp.expressions.util;

import sigmod2021.esp.bindings.BoundVariables;
import sigmod2021.esp.expressions.EvaluableExpression;
import sigmod2021.event.Event;

import java.util.List;

public abstract class EVAbstractBinaryExpression<TIN1, TIN2, TOUT> implements EvaluableExpression<TOUT> {

    private final EvaluableExpression<TIN1> left;
    private final EvaluableExpression<TIN2> right;
    private final OperatorImpl<TIN1, TIN2, TOUT> impl;
    public EVAbstractBinaryExpression(EvaluableExpression<TIN1> left, EvaluableExpression<TIN2> right,
                                      OperatorImpl<TIN1, TIN2, TOUT> impl) {
        this.left = left;
        this.right = right;
        this.impl = impl;
    }

    @Override
    public TOUT eval(Event event, BoundVariables bindings) {
        return impl.apply(left.eval(event, bindings), right.eval(event, bindings));
    }

    @Override
    public TOUT eval(List<Event> previousEvents, Event event, BoundVariables bindings) {
        return impl.apply(left.eval(previousEvents, event, bindings), right.eval(previousEvents, event, bindings));
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((impl == null) ? 0 : impl.hashCode());
        result = prime * result + ((left == null) ? 0 : left.hashCode());
        result = prime * result + ((right == null) ? 0 : right.hashCode());
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
        EVAbstractBinaryExpression<?, ?, ?> other = (EVAbstractBinaryExpression<?, ?, ?>) obj;
        if (impl == null) {
            if (other.impl != null)
                return false;
        } else if (!impl.equals(other.impl))
            return false;
        if (left == null) {
            if (other.left != null)
                return false;
        } else if (!left.equals(other.left))
            return false;
        if (right == null) {
            if (other.right != null)
                return false;
        } else if (!right.equals(other.right))
            return false;
        return true;
    }

    protected interface OperatorImpl<TIN1, TIN2, TOUT> {
        TOUT apply(TIN1 l, TIN2 r);
    }
}
