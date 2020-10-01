package sigmod2021.esp.expressions.arithmetic.constant;

import sigmod2021.esp.expressions.arithmetic.EVNumericExpression;
import sigmod2021.event.Attribute.DataType;

public abstract class EVNumberConstant extends EVConstant<Number> implements EVNumericExpression {

    public EVNumberConstant(Number value) {
        super(value, DataType.fromValue(value));
    }
}
