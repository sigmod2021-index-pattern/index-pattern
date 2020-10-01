package sigmod2021.esp.api.expression.logical;

import sigmod2021.esp.api.expression.BooleanExpression;
import sigmod2021.esp.api.expression.LogicalExpression;
import sigmod2021.esp.api.expression.base.AbstractExpression;

/**
 * Logical value TRUE
 */
public class True extends AbstractExpression<BooleanExpression> implements LogicalExpression {

    public True() {
        super();
    }
}
