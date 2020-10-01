package sigmod2021.esp.bindings;

import java.util.Arrays;

public class BoundVariables {

    public static final BoundVariables EMPTY_BINDINGS = new BoundVariables(0);

//	private final Map<String, Object> bindings = new HashMap<>();

    private final Object[] bindings;

    public BoundVariables(int space) {
        this.bindings = new Object[space];
    }

    public <T> void bind(int idx, T value) {
        bindings[idx] = value;
    }

    public Object get(int idx) {
        return bindings[idx];
    }

    public <T> T get(int idx, Class<T> clazz) throws NoSuchBindingException {
        return clazz.cast(get(idx));
    }

    public BoundVariables copy() {
        BoundVariables result = new BoundVariables(this.bindings.length);
        System.arraycopy(bindings, 0, result.bindings, 0, bindings.length);
        return result;
    }

    @Override
    public String toString() {
        return "VariableBindings " + Arrays.toString(bindings);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Arrays.hashCode(bindings);
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
        BoundVariables other = (BoundVariables) obj;
        if (!Arrays.equals(bindings, other.bindings))
            return false;
        return true;
    }
}
