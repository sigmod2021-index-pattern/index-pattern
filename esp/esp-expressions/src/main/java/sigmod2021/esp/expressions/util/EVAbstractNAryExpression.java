package sigmod2021.esp.expressions.util;


import sigmod2021.esp.expressions.EvaluableExpression;

/**
 *
 */
public abstract class EVAbstractNAryExpression<TIN, TOUT> implements EvaluableExpression<TOUT> {

    private final EvaluableExpression<TIN> inputs[];
    private final OperatorImpl<TIN[], TOUT> impl;
    public EVAbstractNAryExpression(OperatorImpl<TIN[], TOUT> impl, EvaluableExpression<TIN>[] inputs) {
        this.inputs = inputs;
        this.impl = impl;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((impl == null) ? 0 : impl.hashCode());
        result = prime * result + ((inputs == null) ? 0 : inputs.hashCode());
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
        EVAbstractNAryExpression<?, ?> other = (EVAbstractNAryExpression<?, ?>) obj;
        if (impl == null) {
            if (other.impl != null)
                return false;
        } else if (!impl.equals(other.impl))
            return false;
        if (inputs == null) {
            if (other.inputs != null)
                return false;
        } else if (!inputs.equals(other.inputs))
            return false;
        return true;
    }

    protected EvaluableExpression<TIN>[] getInputs() {
        return inputs;
    }

    protected OperatorImpl<TIN[], TOUT> getImpl() {
        return impl;
    }

    protected static interface OperatorImpl<TIN, TOUT> {
        TOUT apply(TIN in);
    }
}
