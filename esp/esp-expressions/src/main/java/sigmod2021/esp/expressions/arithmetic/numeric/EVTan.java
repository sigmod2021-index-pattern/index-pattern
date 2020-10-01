package sigmod2021.esp.expressions.arithmetic.numeric;

import sigmod2021.esp.expressions.arithmetic.EVNumericExpression;
import sigmod2021.event.Attribute.DataType;

public class EVTan extends EVUnaryNumericExpression {

    public EVTan(EVNumericExpression input) {
        super(input, DataType.DOUBLE, (Number x) -> Math.tan(x.doubleValue()));
    }
}
