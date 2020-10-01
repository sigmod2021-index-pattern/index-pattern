package sigmod2021.esp.expressions.arithmetic.numeric;

import sigmod2021.esp.expressions.arithmetic.EVNumericExpression;
import sigmod2021.event.Attribute.DataType;

public class EVATan extends EVUnaryNumericExpression {

    public EVATan(EVNumericExpression input) {
        super(input, DataType.DOUBLE, (Number x) -> Math.atan(x.doubleValue()));
    }
}
