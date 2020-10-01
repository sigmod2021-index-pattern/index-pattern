package sigmod2021.esp.api.epa.pattern.regex;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 *
 */
public class Sequence extends Pattern implements Iterable<Pattern> {

    private final List<Pattern> elements;

    /**
     * Creates a new Sequence instance
     */
    public Sequence(Pattern... elements) {
        this(Arrays.asList(elements));
    }

    public Sequence(List<? extends Pattern> elements) {
        this.elements = new ArrayList<>(elements);
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public Iterator<Pattern> iterator() {
        return elements.iterator();
    }

    public int getNumberOfElements() {
        return elements.size();
    }

    public Pattern getElement(int idx) {
        return elements.get(idx);
    }

    @Override
    public String toString() {
        StringBuilder result = new StringBuilder();
        for (Pattern p : elements) {
            result.append(p);
        }
        return result.toString();
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((this.elements == null) ? 0 : this.elements.hashCode());
        return result;
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Sequence other = (Sequence) obj;
        if (this.elements == null) {
            if (other.elements != null)
                return false;
        } else if (!this.elements.equals(other.elements))
            return false;
        return true;
    }
}
