package sigmod2021.esp.expressions.spatial.geometric;

import com.vividsolutions.jts.geom.Geometry;
import sigmod2021.esp.expressions.arithmetic.EVSpatialExpression;

/**
 *  Calculates the union of the given input geometries.
 *
 *
 */
public class EVSpatialUnion extends EVBinarySpatialExpression {

    public EVSpatialUnion(EVSpatialExpression left, EVSpatialExpression right) {
        super(left, right, (Geometry x, Geometry y) -> x.union(y));
    }

}
