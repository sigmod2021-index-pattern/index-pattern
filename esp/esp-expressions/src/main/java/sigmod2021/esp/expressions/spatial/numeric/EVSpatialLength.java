package sigmod2021.esp.expressions.spatial.numeric;

import com.vividsolutions.jts.geom.Geometry;
import sigmod2021.esp.expressions.arithmetic.EVSpatialExpression;
import sigmod2021.event.Attribute.DataType;

/**
 * Calculates the length of the input geometry.
 *
 *
 */
public class EVSpatialLength extends EVUnarySpatialNumericExpression {

    public EVSpatialLength(EVSpatialExpression input) {
        super(input, DataType.DOUBLE, (Geometry x) -> x.getLength());
    }
}
