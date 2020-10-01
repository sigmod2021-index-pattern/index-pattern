package sigmod2021.esp.expressions.spatial.geometric;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.operation.buffer.BufferOp;
import sigmod2021.esp.api.expression.spatial.geometric.Buffer.CapStyle;
import sigmod2021.esp.expressions.arithmetic.EVSpatialExpression;

/**
 * Calculates a buffer around the input geometry.
 *
 *
 */
public class EVSpatialBuffer extends EVUnarySpatialExpression {

    /** The CapStyle */
    private final CapStyle capStyle;

    /** The buffer's distance */
    private final double distance;


    public EVSpatialBuffer(EVSpatialExpression input, double distance, CapStyle capStyle) {
        super(input, new OperatorImpl<Geometry, Geometry>() {

            @Override
            public Geometry apply(Geometry input) {
                BufferOp op = new BufferOp(input);
                op.setEndCapStyle(capStyle.jtsValue);
                return op.getResultGeometry(distance);
            }
        });
        this.distance = distance;
        this.capStyle = capStyle;

    }

    /**
     * @{inheritDoc
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(super.toString());
        sb.insert(sb.length() - 2, ", " + distance + ", " + capStyle);
        return sb.toString();
    }

}
