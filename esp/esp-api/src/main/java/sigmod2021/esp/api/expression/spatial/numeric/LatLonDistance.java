package sigmod2021.esp.api.expression.spatial.numeric;

import sigmod2021.common.IncompatibleTypeException;
import sigmod2021.esp.api.epa.pattern.symbol.Bindings;
import sigmod2021.esp.api.expression.ArithmeticExpression;
import sigmod2021.esp.api.expression.base.AbstractExpression;
import sigmod2021.event.Attribute.DataType;
import sigmod2021.event.EventSchema;

/**
 * Calculates the distance between the two input geometries.
 *
 *
 */
public class LatLonDistance extends AbstractExpression<ArithmeticExpression> implements ArithmeticExpression {

    /**
     * @param x the first input
     * @param y the second input
     */
    public LatLonDistance(ArithmeticExpression x, ArithmeticExpression y) {
        super(x, y);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DataType getDataType(EventSchema schema, Bindings bindings) throws IncompatibleTypeException {
        return DataType.DOUBLE;
    }
}

