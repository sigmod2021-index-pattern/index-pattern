package sigmod2021.esp.expressions.spatial.geometric;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Point;
import sigmod2021.esp.expressions.arithmetic.EVArithmeticExpression;
import sigmod2021.esp.expressions.arithmetic.EVSpatialExpression;
import sigmod2021.esp.expressions.util.EVAbstractUnaryExpression;
import sigmod2021.event.Attribute.DataType;

/**
 * Calculates a point inside the area of input geometry.
 *
 *
 */
public class EVSpatialInteriorPoint extends EVAbstractUnaryExpression<Geometry, Point>
        implements EVArithmeticExpression<Point> {

    public EVSpatialInteriorPoint(EVSpatialExpression input) {
        super(input, (Geometry x) -> x.getInteriorPoint());
    }

    @Override
    public DataType getType() {
        return DataType.GEOMETRY;
    }
}
