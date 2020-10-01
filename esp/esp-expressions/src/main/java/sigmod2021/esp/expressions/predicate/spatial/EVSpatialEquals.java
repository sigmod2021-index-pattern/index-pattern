package sigmod2021.esp.expressions.predicate.spatial;

import com.vividsolutions.jts.geom.Geometry;
import sigmod2021.esp.expressions.arithmetic.EVSpatialExpression;
import sigmod2021.esp.expressions.predicate.EVPredicate;
import sigmod2021.esp.expressions.util.EVAbstractBinaryExpression;

/**
 * Tests the two input geometries for topological equality as specified by SFS
 * equals.
 */
public class EVSpatialEquals extends EVAbstractBinaryExpression<Geometry, Geometry, Boolean> implements EVPredicate {

    public EVSpatialEquals(EVSpatialExpression left, EVSpatialExpression right) {
        super(left, right, new OperatorImpl<Geometry, Geometry, Boolean>() {

            @Override
            public Boolean apply(Geometry l, Geometry r) {
                return l.equals(r);
            }
        });
    }

}
