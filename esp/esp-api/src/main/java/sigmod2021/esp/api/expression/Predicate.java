package sigmod2021.esp.api.expression;

/**
 * Interface for any predicate.
 */
public interface Predicate extends BooleanExpression {

    /**
     * {@inheritDoc}
     */
    ArithmeticExpression getInput(int index);

}
