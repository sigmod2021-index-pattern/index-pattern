package sigmod2021.esp.expressions.spatial.geometric;


import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryCollection;
import com.vividsolutions.jts.geom.GeometryFactory;
import sigmod2021.esp.bindings.BoundVariables;
import sigmod2021.esp.expressions.EvaluableExpression;
import sigmod2021.esp.expressions.arithmetic.EVSpatialExpression;
import sigmod2021.esp.expressions.util.EVAbstractNAryExpression;
import sigmod2021.event.Attribute.DataType;
import sigmod2021.event.Event;

import java.util.Collections;
import java.util.List;

/**
 *
 */
public class EVSpatialGeometryCollection extends EVAbstractNAryExpression<Geometry, Geometry> implements EVSpatialExpression {

    public EVSpatialGeometryCollection(EvaluableExpression<Geometry>[] inputs) {
        super((Geometry[] x) -> new GeometryCollection(x, new GeometryFactory()), inputs);
    }


    @Override
    public DataType getType() {
        return DataType.GEOMETRY;
    }

    @Override
    public Geometry eval(Event event, BoundVariables bindings) {
        return eval(Collections.emptyList(), event, bindings);
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public Geometry eval(List<Event> previousEvents, Event event, BoundVariables bindings) {
        EvaluableExpression<Geometry>[] inputs = getInputs();
        Geometry[] ev = new Geometry[inputs.length];
        for (int i = 0; i < inputs.length; i++) {
            EvaluableExpression<Geometry> input = inputs[i];
            ev[i] = input.eval(event, bindings);
        }
        return getImpl().apply(ev);
    }
}
