package sigmod2021.esp.expressions.predicate.spatial;

import com.vividsolutions.jts.geom.Geometry;
import sigmod2021.esp.expressions.arithmetic.EVSpatialExpression;
import sigmod2021.esp.expressions.predicate.EVPredicate;
import sigmod2021.esp.expressions.util.EVAbstractBinaryExpression;

/**
 * Tests whether the the distance between the first and second input geometry is
 * less than or equal to a given threshold.
 */
public class EVSpatialIsWithinDistance extends EVAbstractBinaryExpression<Geometry, Geometry, Boolean> implements EVPredicate {

    public EVSpatialIsWithinDistance(EVSpatialExpression left, EVSpatialExpression right, double distance) {
        super(left, right, new OperatorImpl<Geometry, Geometry, Boolean>() {

            @Override
            public Boolean apply(Geometry l, Geometry r) {
                return l.isWithinDistance(r, distance);
            }
        });
    }

}
