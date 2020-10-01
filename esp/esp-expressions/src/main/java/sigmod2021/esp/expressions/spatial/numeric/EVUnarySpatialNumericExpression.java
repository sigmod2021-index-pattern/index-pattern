package sigmod2021.esp.expressions.spatial.numeric;

import com.vividsolutions.jts.geom.Geometry;
import sigmod2021.esp.expressions.EvaluableExpression;
import sigmod2021.esp.expressions.arithmetic.EVNumericExpression;
import sigmod2021.esp.expressions.util.EVAbstractUnaryExpression;
import sigmod2021.event.Attribute.DataType;

public abstract class EVUnarySpatialNumericExpression extends EVAbstractUnaryExpression<Geometry, Number>
        implements EVNumericExpression {

    private final DataType type;

    public EVUnarySpatialNumericExpression(EvaluableExpression<Geometry> input, DataType type, OperatorImpl<Geometry, Number> impl) {
        super(input, impl);
        this.type = type;
    }


    @Override
    public DataType getType() {
        return type;
    }
}

