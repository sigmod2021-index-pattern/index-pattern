package sigmod2021.esp.expressions.predicate.numeric;

import sigmod2021.esp.expressions.arithmetic.EVNumericExpression;
import sigmod2021.esp.expressions.predicate.EVPredicate;
import sigmod2021.esp.expressions.util.EVAbstractTrinaryExpression;
import sigmod2021.event.Attribute.DataType;

import java.util.HashMap;
import java.util.Map;

public class EVNumericBetween extends EVAbstractTrinaryExpression<Number, Number, Number, Boolean> implements EVPredicate {

    static final Map<DataType, OperatorImpl<Number, Number, Number, Boolean>> IMPLS = new HashMap<>();

    static {
        IMPLS.put(DataType.BYTE, (Number v, Number l, Number u) -> l.byteValue() <= v.byteValue() && u.byteValue() >= v.byteValue());
        IMPLS.put(DataType.SHORT, (Number v, Number l, Number u) -> l.shortValue() <= v.shortValue() && u.shortValue() >= v.shortValue());
        IMPLS.put(DataType.INTEGER, (Number v, Number l, Number u) -> l.intValue() <= v.intValue() && u.intValue() >= v.intValue());
        IMPLS.put(DataType.LONG, (Number v, Number l, Number u) -> l.longValue() <= v.longValue() && u.longValue() >= v.longValue());
        IMPLS.put(DataType.FLOAT, (Number v, Number l, Number u) -> l.floatValue() <= v.floatValue() && u.floatValue() >= v.floatValue());
        IMPLS.put(DataType.DOUBLE, (Number v, Number l, Number u) -> l.doubleValue() <= v.doubleValue() && u.doubleValue() >= v.doubleValue());
    }

    public EVNumericBetween(DataType type, EVNumericExpression value, EVNumericExpression lower, EVNumericExpression upper) {
        super(value, lower, upper, IMPLS.get(type));
    }

}
