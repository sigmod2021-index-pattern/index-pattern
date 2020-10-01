package sigmod2021.esp.api.expression.spatial.geometric;

import sigmod2021.common.IncompatibleTypeException;
import sigmod2021.esp.api.epa.pattern.symbol.Bindings;
import sigmod2021.esp.api.expression.ArithmeticExpression;
import sigmod2021.esp.api.expression.base.AbstractExpression;
import sigmod2021.event.Attribute.DataType;
import sigmod2021.event.EventSchema;

/**
 * Calculates the symmetric difference of the given input geometries.
 *
 *
 */
public class SymDifference extends AbstractExpression<ArithmeticExpression> implements ArithmeticExpression {

    /**
     * @param x the first input
     * @param y the seond input
     */
    public SymDifference(ArithmeticExpression x, ArithmeticExpression y) {
        super(x, y);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DataType getDataType(EventSchema schema, Bindings bindings) throws IncompatibleTypeException {
        return DataType.GEOMETRY;
    }
}
