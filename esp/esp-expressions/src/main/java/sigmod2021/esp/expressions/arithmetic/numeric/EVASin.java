package sigmod2021.esp.expressions.arithmetic.numeric;

import sigmod2021.esp.expressions.arithmetic.EVNumericExpression;
import sigmod2021.event.Attribute.DataType;

public class EVASin extends EVUnaryNumericExpression {

    public EVASin(EVNumericExpression input) {
        super(input, DataType.DOUBLE, (Number x) -> Math.asin(x.doubleValue()));
    }
}
