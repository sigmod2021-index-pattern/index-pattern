package sigmod2021.esp.api.epa.pattern;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Holds all variables (attributes and bound variables) used to assemble an output event.
 */
public class Output implements Iterable<String> {

    /**
     * The variable names
     */
    private final List<String> variables = new ArrayList<>();

    /**
     * @param variables The variable names
     */
    public Output(String... variables) {
        for (String f : variables) {
            this.variables.add(f.toUpperCase());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Iterator<String> iterator() {
        return variables.iterator();
    }

    /**
     * @return the number of variables
     */
    public int getSize() {
        return variables.size();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return variables.toString();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((variables == null) ? 0 : variables.hashCode());
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
        Output other = (Output) obj;
        if (variables == null) {
            if (other.variables != null)
                return false;
        } else if (!variables.equals(other.variables))
            return false;
        return true;
    }


}
