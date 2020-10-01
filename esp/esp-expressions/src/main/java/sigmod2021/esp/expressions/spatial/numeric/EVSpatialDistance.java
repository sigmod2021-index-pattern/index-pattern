package sigmod2021.esp.expressions.spatial.numeric;

import com.vividsolutions.jts.geom.Geometry;
import sigmod2021.esp.expressions.arithmetic.EVSpatialExpression;
import sigmod2021.event.Attribute.DataType;

/**
 *  Calculates the distance between the two input geometries.
 *
 *
 */
public class EVSpatialDistance extends EVBinarySpatialNumericExpression {

    public EVSpatialDistance(EVSpatialExpression left, EVSpatialExpression right) {
        super(left, right, DataType.DOUBLE, (Geometry x, Geometry y) -> x.distance(y));
    }
}
