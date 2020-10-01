package sigmod2021.esp.expressions.predicate.spatial;

import com.vividsolutions.jts.geom.Geometry;
import sigmod2021.esp.expressions.arithmetic.EVSpatialExpression;
import sigmod2021.esp.expressions.predicate.EVPredicate;
import sigmod2021.esp.expressions.util.EVAbstractUnaryExpression;

/**
 * Tests whether the input geometry is a rectangle.
 */
public class EVSpatialIsRectangle extends EVAbstractUnaryExpression<Geometry, Boolean> implements EVPredicate {

    public EVSpatialIsRectangle(EVSpatialExpression input) {
        super(input, new OperatorImpl<Geometry, Boolean>() {

            @Override
            public Boolean apply(Geometry input) {
                return input.isRectangle();
            }
        });
    }

}

