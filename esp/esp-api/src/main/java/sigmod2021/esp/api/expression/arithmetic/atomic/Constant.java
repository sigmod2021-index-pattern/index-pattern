package sigmod2021.esp.api.expression.arithmetic.atomic;

import sigmod2021.esp.api.epa.pattern.symbol.Bindings;
import sigmod2021.esp.api.expression.ArithmeticExpression;
import sigmod2021.event.Attribute.DataType;
import sigmod2021.event.EventSchema;

import java.util.Objects;

/**
 * Implementation of a constant value
 *
 * @param <T> the type of the value
 */
public abstract class Constant<T> implements ArithmeticExpression {

    /**
     * The value
     */
    protected final T value;

    /**
     * The data type
     */
    private final DataType type;

    /**
     * @param value The value
     * @param type  The data type
     */
    public Constant(T value, DataType type) {
        this.value = value;
        this.type = type;
    }

    /**
     * @return The value
     */
    public T getValue() {
        return value;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getArity() {
        return 0;
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
    public ArithmeticExpression getInput(int index) {
        throw new IndexOutOfBoundsException("Constant values have no inputs");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DataType getDataType(EventSchema schema, Bindings bindings) {
        return type;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return value == null ? "null" : value.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Constant<?> constant = (Constant<?>) o;
        return Objects.equals(value, constant.value) &&
                type == constant.type;
    }

    @Override
    public int hashCode() {
        return Objects.hash(value, type);
    }
}
