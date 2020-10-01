package sigmod2021.esp.api.expression.spatial.numeric;

import sigmod2021.common.IncompatibleTypeException;
import sigmod2021.esp.api.epa.pattern.symbol.Bindings;
import sigmod2021.esp.api.expression.ArithmeticExpression;
import sigmod2021.esp.api.expression.base.AbstractExpression;
import sigmod2021.event.Attribute.DataType;
import sigmod2021.event.EventSchema;

/**
 * Calculates the number of vertices of the input geometry.
 *
 *
 */
public class NumPoints extends AbstractExpression<ArithmeticExpression> implements ArithmeticExpression {

    /**
     * @param x the input
     */
    public NumPoints(ArithmeticExpression x) {
        super(x);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DataType getDataType(EventSchema schema, Bindings bindings) throws IncompatibleTypeException {
        return DataType.INTEGER;
    }
}

