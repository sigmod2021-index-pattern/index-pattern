package sigmod2021.esp.expressions.predicate.string;

import sigmod2021.esp.expressions.arithmetic.EVStringExpression;
import sigmod2021.esp.expressions.predicate.EVPredicate;
import sigmod2021.esp.expressions.util.EVAbstractBinaryExpression;

public class EVStringLessEq extends EVAbstractBinaryExpression<String, String, Boolean> implements EVPredicate {

    public EVStringLessEq(EVStringExpression left, EVStringExpression right) {
        super(left, right, new OperatorImpl<String, String, Boolean>() {

            @Override
            public Boolean apply(String l, String r) {
                return l.compareTo(r) <= 0;
            }
        });
    }

}
