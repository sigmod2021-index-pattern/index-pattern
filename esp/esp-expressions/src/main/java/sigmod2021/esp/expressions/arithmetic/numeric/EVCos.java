package sigmod2021.esp.expressions.arithmetic.numeric;

import sigmod2021.esp.expressions.arithmetic.EVNumericExpression;
import sigmod2021.event.Attribute.DataType;

public class EVCos extends EVUnaryNumericExpression {

    public EVCos(EVNumericExpression input) {
        super(input, DataType.DOUBLE, (Number x) -> Math.cos(x.doubleValue()));
    }
}
