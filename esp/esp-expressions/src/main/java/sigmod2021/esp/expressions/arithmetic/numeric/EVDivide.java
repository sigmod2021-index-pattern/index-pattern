package sigmod2021.esp.expressions.arithmetic.numeric;

import sigmod2021.esp.expressions.arithmetic.EVNumericExpression;
import sigmod2021.event.Attribute.DataType;

public class EVDivide extends EVBinaryNumericExpression {

    public EVDivide(EVNumericExpression left, EVNumericExpression right) {
        super(left, right, DataType.DOUBLE, (Number x, Number y) -> x.doubleValue() / y.doubleValue());
    }
}
