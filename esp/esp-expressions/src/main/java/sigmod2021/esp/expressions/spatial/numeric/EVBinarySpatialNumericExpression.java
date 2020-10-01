package sigmod2021.esp.expressions.spatial.numeric;

import com.vividsolutions.jts.geom.Geometry;
import sigmod2021.esp.expressions.EvaluableExpression;
import sigmod2021.esp.expressions.arithmetic.EVNumericExpression;
import sigmod2021.esp.expressions.util.EVAbstractBinaryExpression;
import sigmod2021.event.Attribute.DataType;

public abstract class EVBinarySpatialNumericExpression extends EVAbstractBinaryExpression<Geometry, Geometry, Number>
        implements EVNumericExpression {

    private final DataType type;

    public EVBinarySpatialNumericExpression(EvaluableExpression<Geometry> left, EvaluableExpression<Geometry> right, DataType type, OperatorImpl<Geometry, Geometry, Number> impl) {
        super(left, right, impl);
        this.type = type;
    }

    @Override
    public DataType getType() {
        return type;
    }

}
