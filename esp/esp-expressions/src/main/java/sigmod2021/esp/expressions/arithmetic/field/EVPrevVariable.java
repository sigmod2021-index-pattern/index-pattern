package sigmod2021.esp.expressions.arithmetic.field;

import sigmod2021.esp.bindings.BoundVariables;
import sigmod2021.event.Attribute;
import sigmod2021.event.Event;

import java.util.List;

public class EVPrevVariable<T> extends EVVariable<T> {

    private int prev;
    private EVVariable<T> variable;

    public EVPrevVariable(int index, Attribute.DataType type, int prev, EVVariable<T> variable) {
        super(index, type, false);
        this.prev = prev;
        this.variable = variable;
    }

    @Override
    public T eval(Event event, BoundVariables bindings) {
        throw new UnsupportedOperationException("PREV predicate evaluation only allowed with previous events present");
    }

    @Override
    public T eval(List<Event> previousEvents, Event event, BoundVariables bindings) {
        if (prev == 0)
            return variable.eval(event, bindings);
        int totalPrevEvents = previousEvents.size();
        int prevPointer = totalPrevEvents - prev;
        if (prevPointer < 0)
            throw new IllegalArgumentException("Not enough previous events for PREV predicate");
        return variable.eval(previousEvents.get(prevPointer), bindings);
    }

}
