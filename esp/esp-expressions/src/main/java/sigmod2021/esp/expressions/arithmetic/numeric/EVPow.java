package sigmod2021.esp.expressions.arithmetic.numeric;

import sigmod2021.common.IncompatibleTypeException;
import sigmod2021.esp.expressions.arithmetic.EVNumericExpression;
import sigmod2021.event.Attribute.DataType;

import java.util.HashMap;
import java.util.Map;

public class EVPow extends EVBinaryNumericExpression {

    static final OperatorImpl<Number, Number, Number> LONG_IMPL = (Number x, Number y) -> (long) Math.pow(x.longValue(), y.longValue());
    static final OperatorImpl<Number, Number, Number> DOUBLE_IMPL = (Number x, Number y) -> Math.pow(x.doubleValue(), y.doubleValue());

    private static final Map<DataType, OperatorImpl<Number, Number, Number>> IMPLS = new HashMap<>();

    static {
        IMPLS.put(DataType.BYTE, LONG_IMPL);
        IMPLS.put(DataType.SHORT, LONG_IMPL);
        IMPLS.put(DataType.INTEGER, LONG_IMPL);
        IMPLS.put(DataType.LONG, LONG_IMPL);
        IMPLS.put(DataType.FLOAT, DOUBLE_IMPL);
        IMPLS.put(DataType.DOUBLE, DOUBLE_IMPL);
    }

    public EVPow(EVNumericExpression left, EVNumericExpression right) {
        super(left, right, getType(left.getType(), right.getType()), IMPLS.get(getType(left.getType(), right.getType())));
    }

    private static DataType getType(DataType lt, DataType rt) {


        try {
            DataType gct = DataType.getGCT(lt, rt);

            return (gct.ordinal() < DataType.INTEGER.ordinal()) ? DataType.INTEGER : gct;
        } catch (IncompatibleTypeException ite) {
            throw new RuntimeException(ite);
        }
    }
}
