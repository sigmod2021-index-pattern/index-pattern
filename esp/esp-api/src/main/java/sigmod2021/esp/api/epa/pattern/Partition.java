package sigmod2021.esp.api.epa.pattern;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Holds all attributes used to partition an incoming event stream.
 */
public class Partition implements Iterable<String> {

    /**
     * The attribute names
     */
    private final List<String> attributes;

    public Partition(List<String> fields) {
        this.attributes = new ArrayList<>(fields);
    }

    /**
     * @param fields The attribute names
     */
    public Partition(String... fields) {
        this(Arrays.asList(fields));
    }

    /**
     * @return <code>true</code> if there is defined at least one partitioning attribute, <code>false</code> otherwise
     */
    public boolean isEmpty() {
        return attributes.isEmpty();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Iterator<String> iterator() {
        return attributes.iterator();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return attributes.toString();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((attributes == null) ? 0 : attributes.hashCode());
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
        Partition other = (Partition) obj;
        if (attributes == null) {
            if (other.attributes != null)
                return false;
        } else if (!attributes.equals(other.attributes))
            return false;
        return true;
    }


}
