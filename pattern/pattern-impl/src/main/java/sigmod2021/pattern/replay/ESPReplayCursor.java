package sigmod2021.pattern.replay;

import sigmod2021.common.EPException;
import sigmod2021.pattern.util.Input;
import sigmod2021.db.event.PersistentEvent;
import sigmod2021.esp.api.bridge.EPACallback;
import sigmod2021.esp.api.epa.UserDefinedEPA;
import sigmod2021.event.Event;
import sigmod2021.event.EventSchema;
import xxl.core.cursors.Cursor;

import java.util.ArrayDeque;
import java.util.NoSuchElementException;
import java.util.Queue;

/**
 *
 */
public abstract class ESPReplayCursor<T extends UserDefinedEPA> implements Cursor<Event>, EPACallback {

//	private static final Logger log = LoggerFactory.getLogger(ESPReplayCursor.class);

    private final Input<T> in;
    T def;
    private Event next = null;
    private Queue<Event> resultBuffer = new ArrayDeque<>();
    private Cursor<PersistentEvent> inputQuery;
    private long processed;

    /**
     * Creates a new ReplayOperator instance
     */
    public ESPReplayCursor(EventSchema schemaIn, T def) {
        this.in = new Input<>(schemaIn, def);
        this.def = def;
        this.def.setCallback(this);
    }

    protected abstract Cursor<PersistentEvent> getInput(T epa);

    /**
     * @{inheritDoc}
     */
    @Override
    public void open() {
        try {
            this.def.initialize(in);
            this.inputQuery = getInput(this.def);
            this.inputQuery.open();
            next = computeNext();
        } catch (EPException e) {
            throw new RuntimeException("Could not initialize ESPReplayCursor, input query failed.", e);
        }
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public void close() {
        try {
            this.def.destroy(false);
            this.inputQuery.close();
            this.def = null;
            this.inputQuery = null;
        } catch (EPException e) {
            throw new RuntimeException("Could not close ReplayOperator", e);
        }

    }

    private Event computeNext() {
        if (!resultBuffer.isEmpty())
            return resultBuffer.poll();

        while (resultBuffer.isEmpty() && inputQuery.hasNext()) {
            PersistentEvent pe = inputQuery.next();
            this.def.process(in, pe);
            processed++;
        }
        if (!resultBuffer.isEmpty())
            return resultBuffer.poll();

        // Finished
        return null;
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public boolean hasNext() throws IllegalStateException {
        return next != null;
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public Event next() throws IllegalStateException, NoSuchElementException {
        Event result = next;
        next = computeNext();
        return result;
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public Event peek() throws IllegalStateException, NoSuchElementException, UnsupportedOperationException {
        return next;
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public boolean supportsPeek() {
        return true;
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public void remove() throws IllegalStateException, UnsupportedOperationException {
        throw new UnsupportedOperationException("Remove not supported by TPStream.");
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public boolean supportsRemove() {
        return false;
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public void update(Event object) throws IllegalStateException, UnsupportedOperationException {
        throw new UnsupportedOperationException("Update not supported by TPStream.");

    }

    /**
     * @{inheritDoc}
     */
    @Override
    public boolean supportsUpdate() {
        return false;
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public void reset() throws UnsupportedOperationException {
        close();
        open();
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public boolean supportsReset() {
        return true;
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public void receive(Event event) {
        this.resultBuffer.offer(event);
    }

}
