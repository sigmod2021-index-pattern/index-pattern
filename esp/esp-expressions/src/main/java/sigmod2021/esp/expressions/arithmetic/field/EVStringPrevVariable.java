package sigmod2021.esp.expressions.arithmetic.field;

import sigmod2021.esp.expressions.arithmetic.EVStringExpression;
import sigmod2021.event.Attribute;

public class EVStringPrevVariable extends EVPrevVariable<String> implements EVStringExpression {

    public EVStringPrevVariable(int index, Attribute.DataType type, int prev, EVVariable<String> variable) {
        super(index, type, prev, variable);
    }

}
