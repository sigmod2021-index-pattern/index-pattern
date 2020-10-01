package sigmod2021.esp.expressions.arithmetic.string;

import sigmod2021.esp.expressions.arithmetic.EVStringExpression;
import sigmod2021.esp.expressions.util.EVAbstractBinaryExpression;
import sigmod2021.event.Attribute.DataType;

/**
 *
 */
public class EVConcatenate extends EVAbstractBinaryExpression<String, String, String> implements EVStringExpression {

    public EVConcatenate(EVStringExpression left, EVStringExpression right) {
        super(left, right, (String x, String y) -> ((String) x) + " " + ((String) y));
    }

    @Override
    public DataType getType() {
        return DataType.STRING;
    }
}
