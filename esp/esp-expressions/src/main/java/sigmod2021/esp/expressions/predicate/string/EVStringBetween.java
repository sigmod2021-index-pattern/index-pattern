package sigmod2021.esp.expressions.predicate.string;

import sigmod2021.esp.expressions.arithmetic.EVStringExpression;
import sigmod2021.esp.expressions.predicate.EVPredicate;
import sigmod2021.esp.expressions.util.EVAbstractTrinaryExpression;

public class EVStringBetween extends EVAbstractTrinaryExpression<String, String, String, Boolean> implements EVPredicate {

    public EVStringBetween(EVStringExpression value, EVStringExpression lower, EVStringExpression upper) {
        super(value, lower, upper, new OperatorImpl<String, String, String, Boolean>() {

            @Override
            public Boolean apply(String v, String l, String u) {
                return l.compareTo(v) <= 0 && u.compareTo(v) >= 0;
            }
        });
    }

}
