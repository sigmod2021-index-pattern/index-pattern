package sigmod2021.esp.expressions.arithmetic.numeric;

import sigmod2021.esp.expressions.arithmetic.EVNumericExpression;
import sigmod2021.event.Attribute.DataType;

public class EVLog extends EVBinaryNumericExpression {

    public EVLog(EVNumericExpression left, EVNumericExpression right) {
        super(left, right, DataType.DOUBLE, (Number x, Number y) -> Math.log(x.doubleValue()) / Math.log(y.doubleValue()));
    }
}
