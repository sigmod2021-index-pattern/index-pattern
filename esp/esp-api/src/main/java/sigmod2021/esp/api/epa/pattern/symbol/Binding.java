package sigmod2021.esp.api.epa.pattern.symbol;

import sigmod2021.esp.api.expression.ArithmeticExpression;

/**
 * Describes how a variable is bound during processing.
 * The value is an arbitrary arithmetic expression which is
 * evaluated during processing. The result of this evaluation
 * is then bound to the given name.
 * <br>
 * The binding time describes WHEN the binding is executed:
 */
public class Binding {

    /**
     * The name of the variable
     */
    private final String name;
    /**
     * The expression used to compute the variable's value
     */
    private final ArithmeticExpression value;
    /**
     * The binding time
     */
    private final BindingTime bindingTime;

    /**
     * @param name  The name of the variable
     * @param value The expression used to compute the variable's value
     */
    public Binding(String name, ArithmeticExpression value) {
        this(name, value, BindingTime.INITIAL);
    }

    /**
     * @param name        The name of the variable
     * @param value       The expression used to compute the variable's value
     * @param bindingTime The binding time
     */
    public Binding(String name, ArithmeticExpression value, BindingTime bindingTime) {
        this.value = value;
        this.name = name.toUpperCase();
        this.bindingTime = bindingTime;
    }

    /**
     * @return The name of the variable
     */
    public String getName() {
        return name;
    }

    /**
     * @return The expression used to compute the variable's value
     */
    public ArithmeticExpression getValue() {
        return value;
    }

    /**
     * @return The binding time
     */
    public BindingTime getBindingTime() {
        return bindingTime;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return "[" + name + "=" + value.toString() + ", time=" + bindingTime + "]";
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((bindingTime == null) ? 0 : bindingTime.hashCode());
        result = prime * result + ((name == null) ? 0 : name.hashCode());
        result = prime * result + ((value == null) ? 0 : value.hashCode());
        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Binding other = (Binding) obj;
        if (bindingTime != other.bindingTime)
            return false;
        if (name == null) {
            if (other.name != null)
                return false;
        } else if (!name.equals(other.name))
            return false;
        if (value == null) {
            if (other.value != null)
                return false;
        } else if (!value.equals(other.value))
            return false;
        return true;
    }

    /**
     * Describes when a binding is executed
     */
    public static enum BindingTime {
        /**
         * The variable is bound on the first evaluation and then left unchanged
         */
        INITIAL,
        /**
         * The variable is re-bound on each evaluation. E.g. when bound using a kleene+ pattern
         */
        INCREMENTAL
    }
}
