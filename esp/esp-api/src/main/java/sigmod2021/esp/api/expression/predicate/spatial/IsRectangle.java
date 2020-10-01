package sigmod2021.esp.api.expression.predicate.spatial;

import sigmod2021.esp.api.expression.ArithmeticExpression;
import sigmod2021.esp.api.expression.Predicate;
import sigmod2021.esp.api.expression.base.AbstractExpression;

/**
 * Checks if the input is a rectangle 
 *
 *
 */
public class IsRectangle extends AbstractExpression<ArithmeticExpression> implements Predicate {


    /**
     * @param left the first input
     * @param right the second input
     */
    public IsRectangle(ArithmeticExpression input) {
        super(input);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return "(" + getInput(0) + ") is rectangle";
    }


}


