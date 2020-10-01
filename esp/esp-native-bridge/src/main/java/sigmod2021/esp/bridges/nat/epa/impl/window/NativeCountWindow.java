package sigmod2021.esp.bridges.nat.epa.impl.window;

import sigmod2021.esp.api.bridge.EventChannel;
import sigmod2021.esp.api.epa.window.CountWindow;
import sigmod2021.esp.api.util.RingBuffer;
import sigmod2021.event.Event;

/**
 * Implements a count-based window
 */
public class NativeCountWindow extends NativeWindow {

    /**
     * Holds the start timestamp of a slide
     */
    long tstart; // start timestamp of a slide (this timestamp is determined by the last element of a slide)
    /**
     * Tell how many events to delay
     */
    private long delay;
    /**
     * Counts the events at the current time-stamp
     */
    private long globalCounter;
    /**
     * Holds the current clock of the input-stream
     */
    private long clock; // clock of the window (used to set end timestamps of a slide)
    /**
     * Stores delayed events
     */
    private RingBuffer<Event> countBuffer;
    /**
     * Tells whether we are delayed or not
     */
    private boolean isDelayed;

    /**
     * Constructs a new NativeCountWindow-instance
     *
     * @param name     the internal name of this window
     * @param w        the window definition
     * @param schema   the event-schema
     * @param receiver the receiver for results of this window
     */
    public NativeCountWindow(EventChannel input, CountWindow def) {
        super(input, def);
        countBuffer = new RingBuffer<Event>();
        delay = jump;
        while (delay < size)
            // slide until we reach size
            delay += jump;
        delay = delay - size; // difference is number of events we have to delay
        clock = -1;
        tstart = -1;
        isDelayed = false;
    }

    @Override
    public void process(EventChannel input, Event event) {
        long t = event.getT1();
        if (t > clock) { // if time has progressed => Update clock and reset counter
            globalCounter = 0;
            clock = t;
        }
        countBuffer.add(event);

        Event out = null;

        // if initial delay has already happened => perform slide by slide
        if (countBuffer.size() == size + jump) {
            int counter = 0;
            if (delay == 0)
                tstart = countBuffer.get((int) jump - 1).getT1();

            while (countBuffer.size() > size) {
                out = countBuffer.pollFirst();
                if (globalCounter >= size) // 'size' events reported in this slide => ignore the rest
                    continue;
                if (counter < jump - delay) {
                    if (tstart != t) {
                        callback.receive(out.window(tstart, t));
                    }
                    counter++;
                    if (counter == jump - delay) // one delayed SLIDE completed => update tstart
                        tstart = countBuffer.get((int) jump - 1).getT1();
                } else if (tstart != t) {
                    callback.receive(out.window(tstart, t));
                }
                globalCounter++;
            }
        } else if (!isDelayed && countBuffer.size() == size + delay) { // if initial delay has not happened yet, do it now
            tstart = countBuffer.get((int) jump - 1).getT1();
            while (countBuffer.size() > size) {
                out = countBuffer.pollFirst();
                if (tstart != t) {
                    callback.receive(out.window(tstart, t));
                }
                globalCounter++;
            }
            isDelayed = true;
        }
    }

    /**
     * @{inheritDoc}
     */
    @Override
    // TODO: Check Semantics
    public void flushState() {
        if (!countBuffer.isEmpty()) {
            long tend = countBuffer.get(countBuffer.size() - 1).getT1();
            while (countBuffer.size() > jump) {
                tstart = countBuffer.get((int) jump - 1).getT1();
                for (int i = 0; i < jump; i++) {
                    Event out = countBuffer.pollFirst();
                    callback.receive(out.window(tstart, tend));
                }
            }
            while (!countBuffer.isEmpty()) {
                Event out = countBuffer.pollFirst();
                callback.receive(out.window(tend, tend + 1));
            }
        }
    }
}
