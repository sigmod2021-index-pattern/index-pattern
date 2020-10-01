package sigmod2021.esp.expressions.spatial.numeric;

import com.vividsolutions.jts.geom.Geometry;
import sigmod2021.esp.expressions.arithmetic.EVSpatialExpression;
import sigmod2021.event.Attribute.DataType;

/**
 * Calculates area of the input geometry.
 */
public class EVSpatialArea extends EVUnarySpatialNumericExpression {

    public EVSpatialArea(EVSpatialExpression input) {
        super(input, DataType.DOUBLE, (Geometry x) -> x.getArea());
    }

}
