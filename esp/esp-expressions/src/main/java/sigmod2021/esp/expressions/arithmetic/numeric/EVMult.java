package sigmod2021.esp.expressions.arithmetic.numeric;

import sigmod2021.common.IncompatibleTypeException;
import sigmod2021.esp.expressions.arithmetic.EVNumericExpression;
import sigmod2021.esp.expressions.util.EVAbstractBinaryExpression;
import sigmod2021.event.Attribute.DataType;

import java.util.HashMap;
import java.util.Map;

public class EVMult extends EVBinaryNumericExpression {

    static final EVAbstractBinaryExpression.OperatorImpl<Number, Number, Number> INT_IMPL = (Number x, Number y) -> x.intValue() * y.intValue();

    private static final Map<DataType, EVAbstractBinaryExpression.OperatorImpl<Number, Number, Number>> IMPLS = new HashMap<>();

    static {
        IMPLS.put(DataType.BYTE, INT_IMPL);
        IMPLS.put(DataType.SHORT, INT_IMPL);
        IMPLS.put(DataType.INTEGER, INT_IMPL);
        IMPLS.put(DataType.LONG, (Number x, Number y) -> x.longValue() * y.longValue());
        IMPLS.put(DataType.FLOAT, (Number x, Number y) -> x.floatValue() * y.floatValue());
        IMPLS.put(DataType.DOUBLE, (Number x, Number y) -> x.doubleValue() * y.doubleValue());
    }

    public EVMult(EVNumericExpression left, EVNumericExpression right) {
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
