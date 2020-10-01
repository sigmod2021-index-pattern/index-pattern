package sigmod2021.esp.expressions.spatial.numeric;

import com.vividsolutions.jts.geom.Geometry;
import sigmod2021.esp.expressions.arithmetic.EVSpatialExpression;
import sigmod2021.event.Attribute.DataType;

/**
 * Constructs a new SpatialDimension instance
 */
public class EVSpatialDimension extends EVUnarySpatialNumericExpression {

    public EVSpatialDimension(EVSpatialExpression input) {
        super(input, DataType.INTEGER, (Geometry x) -> x.getDimension());
    }
}
