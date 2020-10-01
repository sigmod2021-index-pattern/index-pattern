package sigmod2021.esp.expressions.arithmetic.field;

import sigmod2021.esp.bindings.BoundVariables;
import sigmod2021.event.Attribute.DataType;
import sigmod2021.event.Event;

public class EVDoubleVariable extends EVNumericVariable {

    public EVDoubleVariable(int index, boolean isBinding) {
        super(index, DataType.DOUBLE, isBinding);
    }

    @Override
    public Number eval(Event event, BoundVariables bindings) {
        return isBinding ? bindings.get(index, Double.class) : event.get(index, Double.class);
    }
}
