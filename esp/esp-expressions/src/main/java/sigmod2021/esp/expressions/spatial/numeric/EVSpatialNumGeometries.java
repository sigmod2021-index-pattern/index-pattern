package sigmod2021.esp.expressions.spatial.numeric;

import com.vividsolutions.jts.geom.Geometry;
import sigmod2021.esp.expressions.arithmetic.EVSpatialExpression;
import sigmod2021.event.Attribute.DataType;

/**
 * Calculates the number geometries of the input geometry collection. Returns 1
 * if the input is not a collection.
 *
 *
 */
public class EVSpatialNumGeometries extends EVUnarySpatialNumericExpression {

    public EVSpatialNumGeometries(EVSpatialExpression input) {
        super(input, DataType.INTEGER, (Geometry x) -> x.getNumGeometries());
    }
}
