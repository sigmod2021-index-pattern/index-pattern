package sigmod2021.esp.api.expression.predicate.spatial;

import sigmod2021.esp.api.expression.ArithmeticExpression;
import sigmod2021.esp.api.expression.Predicate;
import sigmod2021.esp.api.expression.base.AbstractExpression;

/**
 * Checks if the input is empty
 *
 *
 */
public class IsEmpty extends AbstractExpression<ArithmeticExpression> implements Predicate {


    /**
     * @param left the first input
     * @param right the second input
     */
    public IsEmpty(ArithmeticExpression input) {
        super(input);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return "(" + getInput(0) + ") is empty";
    }


}



