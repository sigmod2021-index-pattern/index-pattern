package sigmod2021.esp.expressions.spatial.factory;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import sigmod2021.esp.expressions.EvaluableExpression;
import sigmod2021.esp.expressions.arithmetic.EVSpatialExpression;
import sigmod2021.esp.expressions.util.EVAbstractBinaryExpression;
import sigmod2021.event.Attribute;

public class EVPointCreator extends EVAbstractBinaryExpression<Number, Number, Geometry> implements EVSpatialExpression {

    public EVPointCreator(EvaluableExpression<Number> left, EvaluableExpression<Number> right) {
        super(left, right, new EVAbstractBinaryExpression.OperatorImpl<Number, Number, Geometry>() {
            @Override
            public Geometry apply(Number l, Number r) {
                GeometryFactory geometryFactory = new GeometryFactory();
                return geometryFactory.createPoint(new Coordinate(l.doubleValue(), r.doubleValue()));
            }
        });
    }

    @Override
    public Attribute.DataType getType() {
        return Attribute.DataType.GEOMETRY;
    }

}
