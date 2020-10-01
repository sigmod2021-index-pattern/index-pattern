package sigmod2021.esp.expressions.spatial.geometric;

import com.vividsolutions.jts.geom.Geometry;
import sigmod2021.esp.expressions.EvaluableExpression;
import sigmod2021.esp.expressions.arithmetic.EVSpatialExpression;
import sigmod2021.esp.expressions.util.EVAbstractBinaryExpression;
import sigmod2021.event.Attribute.DataType;

/**
 *
 */
public abstract class EVBinarySpatialExpression extends EVAbstractBinaryExpression<Geometry, Geometry, Geometry>
        implements EVSpatialExpression {


    public EVBinarySpatialExpression(EvaluableExpression<Geometry> left, EvaluableExpression<Geometry> right, OperatorImpl<Geometry, Geometry, Geometry> impl) {
        super(left, right, impl);
    }

    @Override
    public DataType getType() {
        return DataType.GEOMETRY;
    }

}

