package sigmod2021.esp.api.expression.spatial.factory;

import sigmod2021.common.IncompatibleTypeException;
import sigmod2021.esp.api.epa.pattern.symbol.Bindings;
import sigmod2021.esp.api.expression.ArithmeticExpression;
import sigmod2021.esp.api.expression.base.AbstractExpression;
import sigmod2021.event.Attribute;
import sigmod2021.event.EventSchema;
import sigmod2021.event.SchemaException;

public class SpatialLineProjection extends AbstractExpression<ArithmeticExpression> implements ArithmeticExpression {

    public SpatialLineProjection(ArithmeticExpression... input) {
        super(input);
    }

    @Override
    public Attribute.DataType getDataType(EventSchema schema, Bindings bindings) throws IncompatibleTypeException, SchemaException {
        return Attribute.DataType.GEOMETRY;
    }

    @Override
    public String toString() {
        if (getArity() == 0)
            return "ST_LineProjection()";
        StringBuilder result = new StringBuilder();
        result.append("ST_LineProjection(");
        result.append(getInput(0).toString());
        for (int i = 1; i < getArity(); i++) {
            result.append(", ");
            result.append(getInput(i).toString());
        }
        result.append(")");
        return result.toString();
    }
}
