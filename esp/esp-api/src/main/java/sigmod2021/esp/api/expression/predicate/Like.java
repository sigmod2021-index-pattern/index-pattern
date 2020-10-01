package sigmod2021.esp.api.expression.predicate;

import sigmod2021.esp.api.expression.ArithmeticExpression;
import sigmod2021.esp.api.expression.Predicate;
import sigmod2021.esp.api.expression.base.AbstractExpression;

/**
 * Compares two inputs for Like Matching.
 */
public class Like extends AbstractExpression<ArithmeticExpression> implements Predicate {

    /**
     * @param left  the input
     * @param right the pattern
     */
    public Like(ArithmeticExpression left, ArithmeticExpression right) {
        super(left, right);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return "(" + getInput(0) + ") LIKE (" + getInput(1) + ")";
    }

}
