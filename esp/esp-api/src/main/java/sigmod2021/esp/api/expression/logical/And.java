package sigmod2021.esp.api.expression.logical;

import sigmod2021.esp.api.expression.BooleanExpression;
import sigmod2021.esp.api.expression.LogicalExpression;
import sigmod2021.esp.api.expression.base.AbstractExpression;

/**
 * Logical AND: Evaluates to true iff both input-expression evaluate to true.
 */
public class And extends AbstractExpression<BooleanExpression> implements LogicalExpression {

    /**
     * @param left  the first input
     * @param right the second input
     */
    public And(BooleanExpression left, BooleanExpression right) {
        super(left, right);
    }
}
