package sigmod2021.esp.api.expression.arithmetic.compund.numeric;

import sigmod2021.common.IncompatibleTypeException;
import sigmod2021.esp.api.epa.pattern.symbol.Bindings;
import sigmod2021.esp.api.expression.ArithmeticExpression;
import sigmod2021.esp.api.expression.base.AbstractExpression;
import sigmod2021.event.Attribute.DataType;
import sigmod2021.event.EventSchema;
import sigmod2021.event.SchemaException;

/**
 * Computes |x|
 */
public class Abs extends AbstractExpression<ArithmeticExpression> implements ArithmeticExpression {

    /**
     * @param x the input
     */
    public Abs(ArithmeticExpression x) {
        super(x);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DataType getDataType(EventSchema schema, Bindings bindings) throws IncompatibleTypeException, SchemaException {
        DataType it = getInput(0).getDataType(schema, bindings);
        if (it.isNumeric())
            return it;
        else
            throw new IncompatibleTypeException("Abs can only be applied to numeric types, input is of type: " + it);
    }
}
