package sigmod2021.esp.expressions.arithmetic.constant;

import sigmod2021.esp.expressions.arithmetic.EVStringExpression;
import sigmod2021.event.Attribute.DataType;

public class EVStringConstant extends EVConstant<String> implements EVStringExpression {

    public EVStringConstant(String value) {
        super(value, DataType.STRING);
    }
}
