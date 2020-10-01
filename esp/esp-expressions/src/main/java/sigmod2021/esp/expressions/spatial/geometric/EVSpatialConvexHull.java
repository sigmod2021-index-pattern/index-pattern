package sigmod2021.esp.expressions.spatial.geometric;

import com.vividsolutions.jts.geom.Geometry;
import sigmod2021.esp.expressions.arithmetic.EVSpatialExpression;

/**
 * Calculates the convex hull of the input geometry.
 *
 *
 */
public class EVSpatialConvexHull extends EVUnarySpatialExpression {

    public EVSpatialConvexHull(EVSpatialExpression input) {
        super(input, (Geometry x) -> x.convexHull());
    }

}
