package sigmod2021.esp.expressions.arithmetic.field;

import sigmod2021.esp.bindings.BoundVariables;
import sigmod2021.event.Attribute.DataType;
import sigmod2021.event.Event;

public class EVShortVariable extends EVNumericVariable {

    public EVShortVariable(int index, boolean isBinding) {
        super(index, DataType.SHORT, isBinding);
    }

    @Override
    public Number eval(Event event, BoundVariables bindings) {
        return isBinding ? bindings.get(index, Short.class) : event.get(index, Short.class);
    }
}
