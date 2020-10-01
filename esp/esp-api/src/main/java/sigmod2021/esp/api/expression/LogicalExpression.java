package sigmod2021.esp.api.expression;

/**
 * Interface for logical operator (e.g. AND, OR, ...)
 */
public interface LogicalExpression extends BooleanExpression {

    /**
     * {@inheritDoc}
     */
    BooleanExpression getInput(int index);

}
