package sigmod2021.esp.api.epa.pattern.symbol;

import sigmod2021.esp.api.epa.PatternMatcher;
import sigmod2021.esp.api.expression.BooleanExpression;

/**
 * A symbol is a symbolic value used by the {@link PatternMatcher} for example.
 * It is produced, if an incoming event satisfies the symbol's condition.
 * <br>
 * Additionally a symbol contains a set of bindings which are used to bind event-values
 * to variables for later use (e.g. to produce a result or to check a condition on subsequent symbols).
 */
public class Symbol {

    /**
     * The symbol's identifier
     */
    private final char identifier;

    /**
     * The condition for this symbol
     */
    private final BooleanExpression condition;

    /**
     * The variable bindings to execute
     */
    private final Bindings bindings;

    /**
     * @param identifier The symbol's identifier
     * @param condition  The condition for this symbol
     */
    public Symbol(char identifier, BooleanExpression condition) {
        this(identifier, condition, new Bindings());
    }

    /**
     * @param identifier The symbol's identifier
     * @param condition  The condition for this symbol
     * @param bindings   the bindings to execute
     */
    public Symbol(char identifier, BooleanExpression condition, Bindings bindings) {
        super();
        this.identifier = identifier;
        this.condition = condition;
        this.bindings = bindings;
    }

    /**
     * @return The identifier
     */
    public char getId() {
        return identifier;
    }

    /**
     * @return The condition
     */
    public BooleanExpression getCondition() {
        return condition;
    }

    /**
     * @return the bindings to execute
     */
    public Bindings getBindings() {
        return bindings;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return "[" + identifier + ":=" + condition + ", bindings=" + bindings + "]";
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((bindings == null) ? 0 : bindings.hashCode());
        result = prime * result + ((condition == null) ? 0 : condition.hashCode());
        result = prime * result + identifier;
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
        Symbol other = (Symbol) obj;
        if (bindings == null) {
            if (other.bindings != null)
                return false;
        } else if (!bindings.equals(other.bindings))
            return false;
        if (condition == null) {
            if (other.condition != null)
                return false;
        } else if (!condition.equals(other.condition))
            return false;
        if (identifier != other.identifier)
            return false;
        return true;
    }
}
