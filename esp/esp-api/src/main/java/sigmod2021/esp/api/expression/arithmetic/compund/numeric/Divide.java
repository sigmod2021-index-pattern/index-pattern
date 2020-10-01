package sigmod2021.esp.api.expression.arithmetic.compund.numeric;

import sigmod2021.common.IncompatibleTypeException;
import sigmod2021.esp.api.epa.pattern.symbol.Bindings;
import sigmod2021.esp.api.expression.ArithmeticExpression;
import sigmod2021.esp.api.expression.base.AbstractExpression;
import sigmod2021.event.Attribute.DataType;
import sigmod2021.event.EventSchema;

/**
 * Computes a / b
 */
public class Divide extends AbstractExpression<ArithmeticExpression> implements ArithmeticExpression {

    /**
     * @param a the first input
     * @param b the second input
     */
    public Divide(ArithmeticExpression a, ArithmeticExpression b) {
        super(a, b);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DataType getDataType(EventSchema schema, Bindings bindings) throws IncompatibleTypeException {
        return DataType.DOUBLE;
    }

    @Override
    public String toString() {
        return "(" + getInput(0) + " / " + getInput(1) + ")";
    }
}
