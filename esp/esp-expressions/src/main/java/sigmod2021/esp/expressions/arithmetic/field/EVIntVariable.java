package sigmod2021.esp.expressions.arithmetic.field;

import sigmod2021.esp.bindings.BoundVariables;
import sigmod2021.event.Attribute.DataType;
import sigmod2021.event.Event;

public class EVIntVariable extends EVNumericVariable {

    public EVIntVariable(int index, boolean isBinding) {
        super(index, DataType.INTEGER, isBinding);
    }

    @Override
    public Number eval(Event event, BoundVariables bindings) {
        return isBinding ? bindings.get(index, Integer.class) : event.get(index, Integer.class);
    }
}
