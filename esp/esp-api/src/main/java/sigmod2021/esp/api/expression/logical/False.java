package sigmod2021.esp.api.expression.logical;

import sigmod2021.esp.api.expression.BooleanExpression;
import sigmod2021.esp.api.expression.LogicalExpression;
import sigmod2021.esp.api.expression.base.AbstractExpression;

/**
 * Logical value FALSE
 */
public class False extends AbstractExpression<BooleanExpression> implements LogicalExpression {

    public False() {
        super();
    }
}
