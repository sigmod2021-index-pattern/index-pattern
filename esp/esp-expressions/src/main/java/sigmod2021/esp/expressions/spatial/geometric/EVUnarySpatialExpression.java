package sigmod2021.esp.expressions.spatial.geometric;

import com.vividsolutions.jts.geom.Geometry;
import sigmod2021.esp.expressions.EvaluableExpression;
import sigmod2021.esp.expressions.arithmetic.EVSpatialExpression;
import sigmod2021.esp.expressions.util.EVAbstractUnaryExpression;
import sigmod2021.event.Attribute.DataType;

/**
 *
 */
public abstract class EVUnarySpatialExpression extends EVAbstractUnaryExpression<Geometry, Geometry>
        implements EVSpatialExpression {


    public EVUnarySpatialExpression(EvaluableExpression<Geometry> input, OperatorImpl<Geometry, Geometry> impl) {
        super(input, impl);
    }


    @Override
    public DataType getType() {
        return DataType.GEOMETRY;
    }
}
