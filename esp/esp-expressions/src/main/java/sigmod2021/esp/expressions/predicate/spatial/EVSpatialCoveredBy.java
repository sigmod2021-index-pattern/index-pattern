package sigmod2021.esp.expressions.predicate.spatial;

import com.vividsolutions.jts.geom.Geometry;
import sigmod2021.esp.expressions.arithmetic.EVSpatialExpression;
import sigmod2021.esp.expressions.predicate.EVPredicate;
import sigmod2021.esp.expressions.util.EVAbstractBinaryExpression;

/**
 * Tests whether the first input geometry is covered by the second one.
 */
public class EVSpatialCoveredBy extends EVAbstractBinaryExpression<Geometry, Geometry, Boolean> implements EVPredicate {

    public EVSpatialCoveredBy(EVSpatialExpression left, EVSpatialExpression right) {
        super(left, right, new OperatorImpl<Geometry, Geometry, Boolean>() {

            @Override
            public Boolean apply(Geometry l, Geometry r) {
                return l.coveredBy(r);
            }
        });
    }

}
