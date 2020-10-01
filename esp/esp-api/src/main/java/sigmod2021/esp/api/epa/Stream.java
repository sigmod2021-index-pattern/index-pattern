package sigmod2021.esp.api.epa;

import sigmod2021.event.EventSchema;
import sigmod2021.event.SchemaException;

/**
 *
 */
public class Stream implements EPA {

    /**
     *
     */
    private static final long serialVersionUID = 1L;

    private final String name;

    /**
     * @param name
     */
    public Stream(String name) {
        this.name = name.toUpperCase();
    }

    /**
     * @return this stream's name
     */
    public String getName() {
        return name;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public EPA[] getInputEPAs() {
        return EMPTY_EPA_ARRAY;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setInput(int idx, EPA input) {
        throw new IllegalArgumentException("No input to set on Stream EPAs");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public EventSchema computeOutputSchema(EventSchema... inputSchemas) throws SchemaException {
        return inputSchemas[0];
    }

    @Override
    public String toString() {
        return "Stream [name=" + name + "]";
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((name == null) ? 0 : name.hashCode());
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
        Stream other = (Stream) obj;
        if (name == null) {
            if (other.name != null)
                return false;
        } else if (!name.equals(other.name))
            return false;
        return true;
    }
}
