package sigmod2021.esp.expressions.spatial.numeric;

import com.vividsolutions.jts.geom.Geometry;
import sigmod2021.esp.expressions.arithmetic.EVSpatialExpression;
import sigmod2021.event.Attribute.DataType;

/**
 * Calculates the number of vertices of the input geometry.
 *
 *
 */
public class EVSpatialNumPoints extends EVUnarySpatialNumericExpression {

    public EVSpatialNumPoints(EVSpatialExpression input) {
        super(input, DataType.INTEGER, (Geometry x) -> x.getNumPoints());
    }
}

