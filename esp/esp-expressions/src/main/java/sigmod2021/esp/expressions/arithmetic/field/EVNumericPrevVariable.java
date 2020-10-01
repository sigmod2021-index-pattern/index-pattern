package sigmod2021.esp.expressions.arithmetic.field;

import sigmod2021.esp.expressions.arithmetic.EVNumericExpression;
import sigmod2021.event.Attribute;

public class EVNumericPrevVariable extends EVPrevVariable<Number> implements EVNumericExpression {

    public EVNumericPrevVariable(int index, Attribute.DataType type, int prev, EVVariable<Number> variable) {
        super(index, type, prev, variable);
    }
}
