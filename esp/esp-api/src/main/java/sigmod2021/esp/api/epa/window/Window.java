package sigmod2021.esp.api.epa.window;

import sigmod2021.esp.api.epa.EPA;
import sigmod2021.event.EventSchema;

/**
 * Base class for modelling window operators
 */
public abstract class Window implements EPA {

    private static final long serialVersionUID = 1L;

    /**
     * The size of this window
     */
    protected final long size;

    /**
     * The jump-size of this window
     */
    protected final long slide;

    /**
     * The input of this window
     */
    protected EPA input;


    /**
     * @param stream The name of the raw stream this window is applied to
     * @param size   The size of this window
     * @param slide  The jump-size of this window
     */
    protected Window(EPA input, long size, long slide) {
        this.input = input;
        this.size = size;
        this.slide = slide;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public void setInput(int idx, EPA input) {
        if (idx == 0)
            this.input = input;
        else
            throw new IllegalArgumentException("Invalid input index: " + idx);
    }

    /**
     * @return The size of this window
     */
    public long getSize() {
        return size;
    }

    /**
     * @return The jump-size of this window
     */
    public long getJump() {
        return slide;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public EventSchema computeOutputSchema(EventSchema... inputSchemas) {
        return inputSchemas[0];
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public EPA[] getInputEPAs() {
        return new EPA[]{input};
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return getClass().getSimpleName() + "[input=" + input + ", size=" + size + ", jump=" + slide + "]";
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int) (size ^ (size >>> 32));
        result = prime * result + ((input == null) ? 0 : input.hashCode());
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
        Window other = (Window) obj;
        if (size != other.size)
            return false;
        if (input == null) {
            if (other.input != null)
                return false;
        } else if (!input.equals(other.input))
            return false;
        return true;
    }
}
