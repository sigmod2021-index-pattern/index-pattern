package sigmod2021.esp.api.expression.spatial.temporal;

import sigmod2021.common.IncompatibleTypeException;
import sigmod2021.esp.api.epa.pattern.symbol.Bindings;
import sigmod2021.esp.api.expression.ArithmeticExpression;
import sigmod2021.esp.api.expression.base.AbstractExpression;
import sigmod2021.event.Attribute;
import sigmod2021.event.EventSchema;
import sigmod2021.event.SchemaException;

public class SpatialTemporalLineCreator extends AbstractExpression<ArithmeticExpression> implements ArithmeticExpression {

    public SpatialTemporalLineCreator(ArithmeticExpression a) {
        super(a);
    }

    @Override
    public Attribute.DataType getDataType(EventSchema schema, Bindings bindings) throws IncompatibleTypeException, SchemaException {
        return Attribute.DataType.GEOMETRY;
    }

    @Override
    public String toString() {
        return "ST_MakeTemporalLine(" + getInput(0) + ")";
    }
}
