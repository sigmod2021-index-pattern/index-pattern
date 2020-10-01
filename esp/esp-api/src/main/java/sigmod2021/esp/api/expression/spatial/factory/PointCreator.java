package sigmod2021.esp.api.expression.spatial.factory;

import sigmod2021.common.IncompatibleTypeException;
import sigmod2021.esp.api.epa.pattern.symbol.Bindings;
import sigmod2021.esp.api.expression.ArithmeticExpression;
import sigmod2021.esp.api.expression.base.AbstractExpression;
import sigmod2021.event.Attribute;
import sigmod2021.event.EventSchema;
import sigmod2021.event.SchemaException;

public class PointCreator extends AbstractExpression<ArithmeticExpression> implements ArithmeticExpression {

    public PointCreator(ArithmeticExpression a, ArithmeticExpression b) {
        super(a, b);
    }

    @Override
    public Attribute.DataType getDataType(EventSchema schema, Bindings bindings) throws IncompatibleTypeException, SchemaException {
        return Attribute.DataType.GEOMETRY;
    }

    @Override
    public String toString() {
        return "asPoint(" + getInput(0) + ", " + getInput(1) + ")";
    }
}
