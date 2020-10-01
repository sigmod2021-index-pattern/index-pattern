package sigmod2021.esp.expressions.arithmetic.field;

import sigmod2021.esp.bindings.BoundVariables;
import sigmod2021.esp.expressions.arithmetic.EVArithmeticExpression;
import sigmod2021.event.Attribute.DataType;
import sigmod2021.event.Event;

import java.util.List;

public abstract class EVVariable<T> implements EVArithmeticExpression<T> {

    protected final int index;

    protected final DataType type;

    protected final boolean isBinding;

    public EVVariable(int index, DataType type, boolean isBinding) {
        this.index = index;
        this.type = type;
        this.isBinding = isBinding;
    }

    @Override
    public T eval(List<Event> previousEvents, Event event, BoundVariables bindings) {
        return eval(event, bindings);
    }

    @Override
    public final DataType getType() {
        return type;
    }
}
