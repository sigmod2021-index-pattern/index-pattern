package sigmod2021.esp.expressions.arithmetic.constant;

import com.vividsolutions.jts.geom.Geometry;
import sigmod2021.esp.expressions.arithmetic.EVSpatialExpression;
import sigmod2021.event.Attribute.DataType;

/**
 *
 */
public class EVSpatialConstant extends EVConstant<Geometry> implements EVSpatialExpression {

    public EVSpatialConstant(Geometry value) {
        super(value, DataType.GEOMETRY);
    }
}

