package sigmod2021.esp.expressions.arithmetic.numeric;

import sigmod2021.esp.expressions.arithmetic.EVNumericExpression;
import sigmod2021.event.Attribute.DataType;

import java.util.HashMap;
import java.util.Map;

public class EVAbs extends EVUnaryNumericExpression {

    private static final Map<DataType, OperatorImpl<Number, Number>> IMPLS = new HashMap<>();

    static {
        IMPLS.put(DataType.BYTE, (Number x) -> (byte) Math.abs(x.intValue()));
        IMPLS.put(DataType.SHORT, (Number x) -> (short) Math.abs(x.intValue()));
        IMPLS.put(DataType.INTEGER, (Number x) -> Math.abs(x.intValue()));
        IMPLS.put(DataType.LONG, (Number x) -> Math.abs(x.longValue()));
        IMPLS.put(DataType.FLOAT, (Number x) -> Math.abs(x.floatValue()));
        IMPLS.put(DataType.DOUBLE, (Number x) -> Math.abs(x.doubleValue()));
    }

    public EVAbs(EVNumericExpression input) {
        super(input, input.getType(), IMPLS.get(input.getType()));
    }

}
