package sigmod2021.esp.api.expression.arithmetic.atomic;

import com.vividsolutions.jts.geom.Geometry;
import sigmod2021.event.Attribute.DataType;

/**
 * Spatial Constant
 *
 *
 */
public class GeoConstant extends Constant<Geometry> {

    /**
     * @param value The value
     */
    public GeoConstant(Geometry value) {
        super(value, DataType.GEOMETRY);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return value == null ? "null" : "'" + value + "'";
    }
}
