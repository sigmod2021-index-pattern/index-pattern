package sigmod2021.esp.expressions.spatial.factory;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import sigmod2021.esp.bindings.BoundVariables;
import sigmod2021.esp.expressions.arithmetic.EVSpatialExpression;
import sigmod2021.event.Attribute;
import sigmod2021.event.Event;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class EVSpatialLineProjection implements EVSpatialExpression {

    private final List<EVSpatialExpression> input;

    public EVSpatialLineProjection(List<EVSpatialExpression> input) {
        this.input = input;
    }

    @Override
    public Attribute.DataType getType() {
        return Attribute.DataType.GEOMETRY;
    }

    @Override
    public Geometry eval(Event event, BoundVariables bindings) {
        return eval(Collections.emptyList(), event, bindings);
    }

    @Override
    public Geometry eval(List<Event> previousEvents, Event event, BoundVariables bindings) {
        GeometryFactory geometryFactory = new GeometryFactory();
        List<Coordinate> results = new ArrayList<>();
        for (EVSpatialExpression evSpatialExpression : input) {
            results.addAll(Arrays.asList(evSpatialExpression.eval(previousEvents, event, bindings).getCoordinates()));
        }
        return geometryFactory.createLineString(results.toArray(new Coordinate[results.size()]));
    }
}
