package sigmod2021.esp.expressions.arithmetic.field;

import sigmod2021.esp.bindings.BoundVariables;
import sigmod2021.esp.expressions.arithmetic.EVSetExpression;
import sigmod2021.event.Attribute.DataType;
import sigmod2021.event.Event;

import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class EVSetVariable extends EVVariable<ArrayList<Object>> implements EVSetExpression {

    public EVSetVariable(int index, boolean isBinding) {
        super(index, DataType.SET, isBinding);
    }

    @Override
    public ArrayList<Object> eval(Event event, BoundVariables bindings) {
        Object o = event.get(index);
        ArrayList<Object> result = new ArrayList<>();
        if (o instanceof ArrayList) {
            for (Object elem : (ArrayList<?>) o)
                result.add(elem);
        } else {
            result.add(o);
        }
        return result;
    }

    public int getIndex() {
        return index;
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public ArrayList<Object> eval(List<Event> previousEvents, Event event, BoundVariables bindings) {
        return eval(event, bindings);
    }

}
