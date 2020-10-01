package sigmod2021.esp.expressions.arithmetic.constant;

import sigmod2021.esp.bindings.BoundVariables;
import sigmod2021.esp.expressions.arithmetic.EVArithmeticExpression;
import sigmod2021.event.Attribute.DataType;
import sigmod2021.event.Event;

import java.util.List;

public abstract class EVConstant<T> implements EVArithmeticExpression<T> {

    private final T value;

    private final DataType type;

    public EVConstant(T value, DataType type) {
        this.value = value;
        this.type = type;
    }

    @Override
    public T eval(Event event, BoundVariables bindings) {
        return value;
    }

    @Override
    public T eval(List<Event> previousEvents, Event event, BoundVariables bindings) {
        return value;
    }

    @Override
    public DataType getType() {
        return type;
    }
}
