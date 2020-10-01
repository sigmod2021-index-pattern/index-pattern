package sigmod2021.esp.expressions.predicate.numeric;

import sigmod2021.esp.expressions.arithmetic.EVNumericExpression;
import sigmod2021.esp.expressions.predicate.EVPredicate;
import sigmod2021.esp.expressions.util.EVAbstractBinaryExpression;
import sigmod2021.event.Attribute.DataType;

import java.util.HashMap;
import java.util.Map;

public class EVNumericGreaterEq extends EVAbstractBinaryExpression<Number, Number, Boolean> implements EVPredicate {

    static final OperatorImpl<Number, Number, Boolean> INT_IMPL = new OperatorImpl<Number, Number, Boolean>() {
        @Override
        public Boolean apply(Number l, Number r) {
            return l.intValue() >= r.intValue();
        }
    };

    static final OperatorImpl<Number, Number, Boolean> LONG_IMPL = new OperatorImpl<Number, Number, Boolean>() {
        @Override
        public Boolean apply(Number l, Number r) {
            return l.longValue() >= r.longValue();
        }
    };

    static final OperatorImpl<Number, Number, Boolean> FLOAT_IMPL = new OperatorImpl<Number, Number, Boolean>() {
        @Override
        public Boolean apply(Number l, Number r) {
            return l.floatValue() >= r.floatValue();
        }
    };

    static final OperatorImpl<Number, Number, Boolean> DOUBLE_IMPL = new OperatorImpl<Number, Number, Boolean>() {
        @Override
        public Boolean apply(Number l, Number r) {
            return l.doubleValue() >= r.doubleValue();
        }
    };

    static final Map<DataType, OperatorImpl<Number, Number, Boolean>> IMPLS = new HashMap<>();

    static {
        IMPLS.put(DataType.BYTE, INT_IMPL);
        IMPLS.put(DataType.SHORT, INT_IMPL);
        IMPLS.put(DataType.INTEGER, INT_IMPL);
        IMPLS.put(DataType.LONG, LONG_IMPL);
        IMPLS.put(DataType.FLOAT, FLOAT_IMPL);
        IMPLS.put(DataType.DOUBLE, DOUBLE_IMPL);
    }

    public EVNumericGreaterEq(DataType type, EVNumericExpression left, EVNumericExpression right) {
        super(left, right, IMPLS.get(type));
    }

}
