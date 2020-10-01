package sigmod2021.esp.expressions.arithmetic.field;

import com.vividsolutions.jts.geom.Geometry;
import sigmod2021.esp.bindings.BoundVariables;
import sigmod2021.esp.expressions.arithmetic.EVSpatialExpression;
import sigmod2021.event.Attribute.DataType;
import sigmod2021.event.Event;

import java.util.List;

/**
 *
 */
public class EVSpatialVariable extends EVVariable<Geometry> implements EVSpatialExpression {

    public EVSpatialVariable(int index, boolean isBinding) {
        super(index, DataType.GEOMETRY, isBinding);
    }

    @Override
    public Geometry eval(Event event, BoundVariables bindings) {
        return isBinding ? bindings.get(index, Geometry.class) : event.get(index, Geometry.class);
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public Geometry eval(List<Event> previousEvents, Event event, BoundVariables bindings) {
        return eval(event, bindings);
    }

}
