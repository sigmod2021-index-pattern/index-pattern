package sigmod2021.esp.api.expression.spatial.numeric;

import sigmod2021.common.IncompatibleTypeException;
import sigmod2021.esp.api.epa.pattern.symbol.Bindings;
import sigmod2021.esp.api.expression.ArithmeticExpression;
import sigmod2021.esp.api.expression.base.AbstractExpression;
import sigmod2021.event.Attribute.DataType;
import sigmod2021.event.EventSchema;

/**
 * Calculates the number geometries of the input geometry collection. Returns 1
 * if the input is not a collection.
 *
 *
 */
public class NumGeometries extends AbstractExpression<ArithmeticExpression> implements ArithmeticExpression {

    /**
     * @param x the input
     */
    public NumGeometries(ArithmeticExpression x) {
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
