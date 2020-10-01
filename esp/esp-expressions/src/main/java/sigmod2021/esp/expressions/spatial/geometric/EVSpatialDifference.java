package sigmod2021.esp.expressions.spatial.geometric;

import com.vividsolutions.jts.geom.Geometry;
import sigmod2021.esp.expressions.arithmetic.EVSpatialExpression;

/**
 * Calculates the difference of the given input geometries.
 *
 *
 */
public class EVSpatialDifference extends EVBinarySpatialExpression {

    public EVSpatialDifference(EVSpatialExpression left, EVSpatialExpression right) {
        super(left, right, (Geometry x, Geometry y) -> x.difference(y));
    }

}
