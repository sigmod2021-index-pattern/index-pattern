package sigmod2021.esp.expressions.bool;

import sigmod2021.esp.bindings.BoundVariables;
import sigmod2021.event.Event;

import java.util.List;

public class EVFalse implements EVBooleanExpression {

    @Override
    public Boolean eval(Event event, BoundVariables bindings) {
        return false;
    }

    @Override
    public Boolean eval(List<Event> previousEvents, Event event, BoundVariables bindings) {
        return false;
    }
}
