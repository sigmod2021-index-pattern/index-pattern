package sigmod2021.esp.expressions.spatial.geometric;

import com.vividsolutions.jts.geom.Geometry;
import sigmod2021.esp.expressions.arithmetic.EVSpatialExpression;
import sigmod2021.esp.expressions.util.EVAbstractUnaryExpression;

/**
 * Retrieves the n-th geometry out of a geometry collection. The input geometry
 * is returned if it is not a collection.
 *
 *
 */
public class EVSpatialGeometryN extends EVUnarySpatialExpression {

    private final int n;

    public EVSpatialGeometryN(EVSpatialExpression input, int n) {
        super(input, new EVAbstractUnaryExpression.OperatorImpl<Geometry, Geometry>() {

            @Override
            public Geometry apply(Geometry input) {
                return input.getGeometryN(n);
            }
        });
        this.n = n;
    }

    /**
     * @{inheritDoc
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(super.toString());
        sb.insert(sb.length() - 2, ", " + n);
        return sb.toString();
    }

}
