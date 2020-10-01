package sigmod2021.esp.expressions.spatial.numeric;

import com.vividsolutions.jts.geom.Geometry;
import sigmod2021.esp.expressions.arithmetic.EVSpatialExpression;
import sigmod2021.event.Attribute.DataType;

/**
 * Calculates boundary-dimension of the input geometry.
 */
public class EVSpatialBoundaryDimension extends EVUnarySpatialNumericExpression {

    public EVSpatialBoundaryDimension(EVSpatialExpression input) {
        super(input, DataType.INTEGER, (Geometry x) -> x.getBoundaryDimension());
    }
}
