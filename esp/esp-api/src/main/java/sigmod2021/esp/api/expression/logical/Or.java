package sigmod2021.esp.api.expression.logical;

import sigmod2021.esp.api.expression.BooleanExpression;
import sigmod2021.esp.api.expression.LogicalExpression;
import sigmod2021.esp.api.expression.base.AbstractExpression;

/**
 * Logical AND: Evaluates to true if at least one of the input-expressions evaluates to true
 */
public class Or extends AbstractExpression<BooleanExpression> implements LogicalExpression {

    /**
     * @param left  the first input
     * @param right the second input
     */
    public Or(BooleanExpression left, BooleanExpression right) {
        super(left, right);
    }
}
