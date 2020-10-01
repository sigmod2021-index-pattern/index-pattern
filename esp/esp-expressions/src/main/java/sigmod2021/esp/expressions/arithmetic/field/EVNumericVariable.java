package sigmod2021.esp.expressions.arithmetic.field;

import sigmod2021.esp.expressions.arithmetic.EVNumericExpression;
import sigmod2021.event.Attribute.DataType;

public abstract class EVNumericVariable extends EVVariable<Number> implements EVNumericExpression {

    public EVNumericVariable(int index, DataType type, boolean isBinding) {
        super(index, type, isBinding);
    }
}
