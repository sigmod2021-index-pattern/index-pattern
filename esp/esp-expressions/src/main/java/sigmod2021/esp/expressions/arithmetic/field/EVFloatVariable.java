package sigmod2021.esp.expressions.arithmetic.field;

import sigmod2021.esp.bindings.BoundVariables;
import sigmod2021.event.Attribute.DataType;
import sigmod2021.event.Event;

public class EVFloatVariable extends EVNumericVariable {

    public EVFloatVariable(int index, boolean isBinding) {
        super(index, DataType.FLOAT, isBinding);
    }

    @Override
    public Number eval(Event event, BoundVariables bindings) {
        return isBinding ? bindings.get(index, Float.class) : event.get(index, Float.class);
    }
}
