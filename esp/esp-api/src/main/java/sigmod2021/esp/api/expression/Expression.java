package sigmod2021.esp.api.expression;

/**
 * Interface for any expression.
 */
public interface Expression {

    default int getMaximumRequiredNumberOfPreviousEvents() {
        int result = 0;
        for (int i = 0; i < getArity(); i++) {
            result = Math.max(result, getInput(i).getRequiredNumberOfPreviousEvents());
        }
        return result + getRequiredNumberOfPreviousEvents();
    }

    int getRequiredNumberOfPreviousEvents();

    /**
     * @return the arity of this expression
     */
    int getArity();

    /**
     * @param index the index of the child-expression
     * @return The index-th child-expression
     */
    Expression getInput(int index);

}
