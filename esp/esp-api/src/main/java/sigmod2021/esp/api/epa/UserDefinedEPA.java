package sigmod2021.esp.api.epa;

import sigmod2021.common.EPException;
import sigmod2021.esp.api.bridge.EPACallback;
import sigmod2021.esp.api.bridge.EventChannel;
import sigmod2021.event.Event;

import java.util.Arrays;

/**
 * Base class for defining user defined EPAs.
 * Implementing classes must take care, that the correct {@link EventSchema output-schema}
 * is computed by the {@link #init(EventStream...)}  method.
 * <br>
 * Results are reported using the {@link #publishResult(Event)} method
 */

/**
 *
 */
public abstract class UserDefinedEPA implements EPA {

    private static final long serialVersionUID = 1L;

    /** The input-EPAs */
    private final EPA[] inputs;

    /** The callback to notify the system about results */
    private EPACallback callback;

    /**
     * @param input all input-EPAs
     */
    protected UserDefinedEPA(EPA... input) {
        this.inputs = new EPA[input.length];
        System.arraycopy(input, 0, inputs, 0, input.length);
    }

    /**
     * Initializes the operator and stores
     * the output-schema for later use
     * @param inputStreams references to all input-streams (including their schemas).
     */
    public abstract void initialize(EventChannel... inputStreams) throws EPException;

    /**
     * Processs the given event
     * @param input the input-stream the event belongs to
     * @param event the event to process
     */
    public abstract void process(EventChannel input, Event event);

    /**
     * Shuts down this EPA. Any further use of its methods will result in undefined behaviour
     */
    public abstract void destroy(boolean flush) throws EPException;


    /**
     * Callback for implementor to publish results back to the system
     * @param event the event to publish
     */
    protected final void publishResult(Event event) {
        callback.receive(event);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final EPA[] getInputEPAs() {
        EPA[] result = new EPA[inputs.length];
        System.arraycopy(inputs, 0, result, 0, inputs.length);
        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setInput(int idx, EPA input) {
        if (idx <= inputs.length)
            this.inputs[idx] = input;
        else
            throw new IllegalArgumentException("Invalid input index: " + idx);
    }

    /**
     * @param callback the callback used to notify the system about results
     */
    public final void setCallback(EPACallback callback) {
        this.callback = callback;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return getClass().getSimpleName() + "[inputs=" + Arrays.toString(inputs) + "]";
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((callback == null) ? 0 : callback.hashCode());
        result = prime * result + Arrays.hashCode(inputs);
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
        UserDefinedEPA other = (UserDefinedEPA) obj;
        if (callback == null) {
            if (other.callback != null)
                return false;
        } else if (!callback.equals(other.callback))
            return false;
        if (!Arrays.equals(inputs, other.inputs))
            return false;
        return true;
    }
}
