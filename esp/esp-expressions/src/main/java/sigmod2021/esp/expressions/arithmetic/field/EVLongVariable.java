package sigmod2021.esp.expressions.arithmetic.field;

import sigmod2021.esp.bindings.BoundVariables;
import sigmod2021.event.Attribute.DataType;
import sigmod2021.event.Event;

public class EVLongVariable extends EVNumericVariable {

    public EVLongVariable(int index, boolean isBinding) {
        super(index, DataType.LONG, isBinding);
    }

    @Override
    public Number eval(Event event, BoundVariables bindings) {
        return isBinding ? bindings.get(index, Long.class) : event.get(index, Long.class);
    }
}
