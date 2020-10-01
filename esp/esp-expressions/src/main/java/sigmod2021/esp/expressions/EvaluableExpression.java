package sigmod2021.esp.expressions;

import sigmod2021.esp.bindings.BoundVariables;
import sigmod2021.event.Event;

import java.util.List;

public interface EvaluableExpression<T> {

    default T eval(Event event) {
        return eval(event, BoundVariables.EMPTY_BINDINGS);
    }

    T eval(Event event, BoundVariables bindings);

    T eval(List<Event> previousEvents, Event event, BoundVariables bindings);

}
