package sigmod2021.esp.expressions.arithmetic.numeric;

import sigmod2021.esp.expressions.arithmetic.EVNumericExpression;
import sigmod2021.event.Attribute.DataType;

public class EVExp extends EVUnaryNumericExpression {

    public EVExp(EVNumericExpression input) {
        super(input, DataType.DOUBLE, (Number x) -> Math.exp(x.doubleValue()));
    }
}
