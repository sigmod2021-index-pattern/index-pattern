package sigmod2021.esp.api.expression.arithmetic.compund.numeric;

import sigmod2021.common.IncompatibleTypeException;
import sigmod2021.esp.api.epa.pattern.symbol.Bindings;
import sigmod2021.esp.api.expression.ArithmeticExpression;
import sigmod2021.esp.api.expression.base.AbstractExpression;
import sigmod2021.event.Attribute.DataType;
import sigmod2021.event.EventSchema;

/**
 * Computes the sin of x
 */
public class Sin extends AbstractExpression<ArithmeticExpression> implements ArithmeticExpression {

    /**
     * @param x the input
     */
    public Sin(ArithmeticExpression x) {
        super(x);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DataType getDataType(EventSchema schema, Bindings bindings) throws IncompatibleTypeException {
        return DataType.DOUBLE;
    }
}
