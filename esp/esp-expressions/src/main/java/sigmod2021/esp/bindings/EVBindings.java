package sigmod2021.esp.bindings;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 *
 */
public class EVBindings implements Iterable<EVBinding> {

    private final List<EVBinding> bindings = new ArrayList<>();

    public EVBindings(List<EVBinding> bindings) {
        this.bindings.addAll(bindings);
    }

    public EVBindings(EVBinding... bindings) {
        this(Arrays.asList(bindings));
    }

    public int getNumberOfBindings() {
        return bindings.size();
    }

    public EVBinding getBinding(int idx) {
        return bindings.get(idx);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Iterator<EVBinding> iterator() {
        return bindings.iterator();
    }

    @Override
    public String toString() {
        return "EVBindings [bindings=" + bindings + "]";
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((bindings == null) ? 0 : bindings.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        EVBindings other = (EVBindings) obj;
        if (bindings == null) {
            if (other.bindings != null)
                return false;
        } else if (!bindings.equals(other.bindings))
            return false;
        return true;
    }


}
