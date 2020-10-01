package sigmod2021.esp.api.expression.logical;

import sigmod2021.esp.api.expression.BooleanExpression;
import sigmod2021.esp.api.expression.LogicalExpression;
import sigmod2021.esp.api.expression.base.AbstractExpression;

/**
 * Logical NOT. Consumes a boolean expression and negates its result
 */
public class Not extends AbstractExpression<BooleanExpression> implements LogicalExpression {

    /**
     * @param input the input expression
     */
    public Not(BooleanExpression input) {
        super(input);
    }
}
