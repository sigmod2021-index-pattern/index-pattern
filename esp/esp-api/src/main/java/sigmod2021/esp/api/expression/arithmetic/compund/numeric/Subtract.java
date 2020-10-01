package sigmod2021.esp.api.expression.arithmetic.compund.numeric;

import sigmod2021.common.IncompatibleTypeException;
import sigmod2021.esp.api.epa.pattern.symbol.Bindings;
import sigmod2021.esp.api.expression.ArithmeticExpression;
import sigmod2021.esp.api.expression.base.AbstractExpression;
import sigmod2021.event.Attribute.DataType;
import sigmod2021.event.EventSchema;
import sigmod2021.event.SchemaException;

/**
 * Computes a - b
 */
public class Subtract extends AbstractExpression<ArithmeticExpression> implements ArithmeticExpression {

    /**
     * @param a the first input
     * @param b the second input
     */
    public Subtract(ArithmeticExpression a, ArithmeticExpression b) {
        super(a, b);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DataType getDataType(EventSchema schema, Bindings bindings) throws IncompatibleTypeException, SchemaException {
        DataType l = getInput(0).getDataType(schema, bindings);
        DataType r = getInput(1).getDataType(schema, bindings);
        DataType gct = DataType.getGCT(l, r);
        return (gct.compareTo(DataType.INTEGER) < 0) ? DataType.INTEGER : gct;
    }

    @Override
    public String toString() {
        return "(" + getInput(0) + " - " + getInput(1) + ")";
    }


}
