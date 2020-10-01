package sigmod2021.esp.expressions.arithmetic.field;

import sigmod2021.esp.bindings.BoundVariables;
import sigmod2021.esp.expressions.arithmetic.EVStringExpression;
import sigmod2021.event.Attribute.DataType;
import sigmod2021.event.Event;

public class EVStringVariable extends EVVariable<String> implements EVStringExpression {

    public EVStringVariable(int index, boolean isBinding) {
        super(index, DataType.STRING, isBinding);
    }

    @Override
    public String eval(Event event, BoundVariables bindings) {
        return isBinding ? bindings.get(index, String.class) : event.get(index, String.class);
    }
}
