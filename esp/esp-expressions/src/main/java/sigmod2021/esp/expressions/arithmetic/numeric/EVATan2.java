package sigmod2021.esp.expressions.arithmetic.numeric;

import sigmod2021.esp.expressions.arithmetic.EVNumericExpression;
import sigmod2021.event.Attribute.DataType;

public class EVATan2 extends EVBinaryNumericExpression {

    public EVATan2(EVNumericExpression y, EVNumericExpression x) {
        super(y, x, DataType.DOUBLE, (Number y1, Number x1) -> Math.atan2(y1.doubleValue(), x1.doubleValue()));
    }
}
