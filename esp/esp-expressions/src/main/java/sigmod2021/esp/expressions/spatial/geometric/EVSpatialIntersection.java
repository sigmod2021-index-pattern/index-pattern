package sigmod2021.esp.expressions.spatial.geometric;

import com.vividsolutions.jts.geom.Geometry;
import sigmod2021.esp.expressions.arithmetic.EVSpatialExpression;

/**
 * Calculates the intersection of the given input geometries.
 *
 *
 */
public class EVSpatialIntersection extends EVBinarySpatialExpression {

    public EVSpatialIntersection(EVSpatialExpression left, EVSpatialExpression right) {
        super(left, right, (Geometry x, Geometry y) -> x.intersection(y));
    }

}
