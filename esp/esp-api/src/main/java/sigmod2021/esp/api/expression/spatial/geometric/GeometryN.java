package sigmod2021.esp.api.expression.spatial.geometric;

import sigmod2021.common.IncompatibleTypeException;
import sigmod2021.esp.api.epa.pattern.symbol.Bindings;
import sigmod2021.esp.api.expression.ArithmeticExpression;
import sigmod2021.esp.api.expression.base.AbstractExpression;
import sigmod2021.event.Attribute.DataType;
import sigmod2021.event.EventSchema;

/**
 * Retrieves the n-th geometry out of a geometry collection. The input geometry
 * is returned if it is not a collection.
 *
 *
 */
public class GeometryN extends AbstractExpression<ArithmeticExpression> implements ArithmeticExpression {

    private final int n;

    /**
     * @param x the input
     */
    public GeometryN(ArithmeticExpression x, int n) {
        super(x);
        this.n = n;
    }

    public int getN() {
        return n;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DataType getDataType(EventSchema schema, Bindings bindings) throws IncompatibleTypeException {
        return DataType.GEOMETRY;
    }
}


