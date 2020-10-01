package sigmod2021.esp.api.expression.base;

import sigmod2021.esp.api.expression.Expression;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Abstract implementation of an expression with homogeneous input types
 *
 * @param <T> the type of input expressions
 */
public abstract class AbstractExpression<T extends Expression> implements Expression {

    /**
     * The input expressions
     */
    private final List<T> input;

    /**
     * @param inputs the input-expressions
     */
    @SafeVarargs
    public AbstractExpression(T... inputs) {
        this.input = new ArrayList<>(Arrays.asList(inputs));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getArity() {
        return input.size();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public T getInput(int index) {
        return input.get(index);
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public int getRequiredNumberOfPreviousEvents() {
        return 0;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        if (input.isEmpty()) {
            return getClass().getSimpleName();
        } else {
            StringBuilder sb = new StringBuilder(getClass().getSimpleName());
            sb.append("(");
            for (T i : input) {
                sb.append(i).append(", ");
            }
            sb.delete(sb.length() - 2, sb.length());
            sb.append(")");
            return sb.toString();
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        AbstractExpression<?> that = (AbstractExpression<?>) o;

        return input != null ? input.equals(that.input) : that.input == null;
    }

    @Override
    public int hashCode() {
        return input != null ? input.hashCode() : 0;
    }
}
