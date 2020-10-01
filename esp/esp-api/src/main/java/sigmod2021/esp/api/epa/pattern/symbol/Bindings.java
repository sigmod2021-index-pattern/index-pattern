package sigmod2021.esp.api.epa.pattern.symbol;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Holds a set of {@link Binding bindings}.
 */
public class Bindings implements Iterable<Binding> {

    /**
     * The bindings
     */
    private final List<Binding> bindings = new ArrayList<>();

    /**
     * @param bindings the bindings
     */
    public Bindings(Binding... bindings) {
        this.bindings.addAll(Arrays.asList(bindings));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Iterator<Binding> iterator() {
        return bindings.iterator();
    }

    /**
     * @return the number of bindings
     */
    public int getNumberOfBindings() {
        return bindings.size();
    }

    public int indexOf(String name) {
        String uName = name.toUpperCase();
        for (int i = 0; i < bindings.size(); i++) {
            if (bindings.get(i).getName().equals(uName))
                return i;
        }
        return -1;
    }

    /**
     * Adds a set of bindings to this bindings
     *
     * @param bs the bindings to add
     * @throws DuplicateBindingException if the given bindings contains a variable name which is already used
     */
    public void add(Bindings bs) throws DuplicateBindingException {
        for (Binding b : bs) {
            if (!containsBinding(b))
                bindings.add(b);
            else
                throw new DuplicateBindingException("Variable " + b.getName() + " already bound");
        }
    }

    /**
     * Helper to check if the given binding shadowing an existing binding
     *
     * @param nb the binding to check
     * @return <code>true</code> if an existing binding would be shadowed, <code>false</code> otherwise
     */
    private boolean containsBinding(Binding nb) {
        for (Binding b : bindings)
            if (b.getName().equals(nb.getName()))
                return true;
        return false;
    }

    /**
     * Retrieves the binding for the given variable name
     *
     * @param name the variable name
     * @return the binding for the given name
     * @throws NoSuchVariableException if no such binding is defined
     */
    public Binding byName(String name) throws NoSuchVariableException {
        String uName = name.toUpperCase();
        for (Binding b : bindings)
            if (b.getName().equals(uName))
                return b;
        throw new NoSuchVariableException("Variable " + uName + " not bound!");
    }

    /**
     * Returns a copy of this bindings, excluding the given one.
     *
     * @param w the binding to exclude
     * @return a copy of this bindings, excluding the given one.
     */
    public Bindings without(final Binding w) {
        Bindings result = new Bindings();
        for (Binding b : bindings) {
            if (!w.equals(b))
                result.bindings.add(b);
        }
        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((bindings == null) ? 0 : bindings.hashCode());
        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return bindings.toString();
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
        Bindings other = (Bindings) obj;
        if (bindings == null) {
            if (other.bindings != null)
                return false;
        } else if (!bindings.equals(other.bindings))
            return false;
        return true;
    }
}
