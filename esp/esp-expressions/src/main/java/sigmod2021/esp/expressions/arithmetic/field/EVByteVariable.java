package sigmod2021.esp.expressions.arithmetic.field;

import sigmod2021.esp.bindings.BoundVariables;
import sigmod2021.event.Attribute.DataType;
import sigmod2021.event.Event;

public class EVByteVariable extends EVNumericVariable {

    public EVByteVariable(int index, boolean isBinding) {
        super(index, DataType.BYTE, isBinding);
    }

    @Override
    public Number eval(Event event, BoundVariables bindings) {
        return isBinding ? bindings.get(index, Byte.class) : event.get(index, Byte.class);
    }
}
