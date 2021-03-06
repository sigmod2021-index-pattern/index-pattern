package sigmod2021.esp.expressions.predicate.set;

import sigmod2021.esp.expressions.arithmetic.EVSetExpression;
import sigmod2021.esp.expressions.predicate.EVPredicate;
import sigmod2021.esp.expressions.util.EVAbstractBinaryExpression;

import java.util.ArrayList;

/**
 *
 */
public class EVSubset extends EVAbstractBinaryExpression<ArrayList<Object>, ArrayList<Object>, Boolean> implements EVPredicate {

    public EVSubset(EVSetExpression left, EVSetExpression right) {
        super(left, right, new OperatorImpl<ArrayList<Object>, ArrayList<Object>, Boolean>() {

            @Override
            public Boolean apply(ArrayList<Object> l, ArrayList<Object> r) {
                return r.containsAll(l);
            }
        });
    }

}
