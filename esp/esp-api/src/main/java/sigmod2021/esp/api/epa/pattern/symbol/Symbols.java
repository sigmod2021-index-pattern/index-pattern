package sigmod2021.esp.api.epa.pattern.symbol;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Holds a set of symbol definitions
 */
public class Symbols implements Iterable<Symbol> {

    /**
     * The symbols
     */
    private final List<Symbol> symbols = new ArrayList<>();

    /**
     * @param symbols The symbols
     */
    public Symbols(List<Symbol> symbols) {
        this.symbols.addAll(symbols);
    }

    /**
     * @param symbols The symbols
     */
    public Symbols(Symbol... symbols) {
        this.symbols.addAll(Arrays.asList(symbols));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Iterator<Symbol> iterator() {
        return symbols.iterator();
    }

    /**
     * @return The number of symbols
     */
    public int getNumberOfSymbols() {
        return symbols.size();
    }

    /**
     * Retrieves the symbol at the given position
     *
     * @param idx the index of the symbol to retrieve
     * @return the symbol at the given idx
     */
    public Symbol getSymbol(int idx) {
        return symbols.get(idx);
    }

    public Symbol getSymbolById(char s) {
        return symbols.get(getSymbolIndex(s));
    }

    /**
     * Retrieves the index of the symbol with the given identifier
     *
     * @param s the symbol id to lookup
     * @return the index of the symbol with the given identifier, -1 if no symbol with the given id can be found
     */
    public int getSymbolIndex(char s) {
        int idx = 0;
        for (Symbol sym : symbols) {
            if (sym.getId() == s)
                return idx;
            idx++;
        }
        return -1;
    }


    /**
     * Retrieves the binding with the given variable-name
     *
     * @param variable the name of the variable
     * @return the binding with the given variable-name
     * @throws NoSuchVariableException if no binding with the given variable is defined
     */
    public Binding findBinding(String variable) throws NoSuchVariableException {
        for (Symbol s : symbols) {
            for (Binding b : s.getBindings()) {
                if (b.getName().equals(variable))
                    return b;
            }
        }
        throw new NoSuchVariableException("Variable: " + variable + " not bound,");
    }

    /**
     * @return A list of all bindings defined by the symbol-definitions stored in this set
     * @throws DuplicateBindingException if a binding occurs in more than one symbol
     */
    public Bindings getAllBindings() throws DuplicateBindingException {
        Bindings result = new Bindings();
        for (Symbol s : symbols)
            result.add(s.getBindings());
        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return symbols.toString();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((symbols == null) ? 0 : symbols.hashCode());
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
        Symbols other = (Symbols) obj;
        if (symbols == null) {
            if (other.symbols != null)
                return false;
        } else if (!symbols.equals(other.symbols))
            return false;
        return true;
    }
}
