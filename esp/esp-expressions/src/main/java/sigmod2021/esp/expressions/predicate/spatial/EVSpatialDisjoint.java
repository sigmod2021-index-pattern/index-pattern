package sigmod2021.esp.expressions.predicate.spatial;

import com.vividsolutions.jts.geom.Geometry;
import sigmod2021.esp.expressions.arithmetic.EVSpatialExpression;
import sigmod2021.esp.expressions.predicate.EVPredicate;
import sigmod2021.esp.expressions.util.EVAbstractBinaryExpression;

/**
 * Tests whether the two input geometries are disjoint.
 */
public class EVSpatialDisjoint extends EVAbstractBinaryExpression<Geometry, Geometry, Boolean> implements EVPredicate {

    public EVSpatialDisjoint(EVSpatialExpression left, EVSpatialExpression right) {
        super(left, right, new OperatorImpl<Geometry, Geometry, Boolean>() {

            @Override
            public Boolean apply(Geometry l, Geometry r) {
                return l.disjoint(r);
            }
        });
    }

}
