package sigmod2021.esp.api.epa.window;

import sigmod2021.esp.api.epa.EPA;

/**
 * Representation of a time-based window EPA.
 */
public class TimeWindow extends Window {

    private static final long serialVersionUID = 1L;


    /**
     * @param input The input EPA of this window
     * @param size  The size of this window
     */
    public TimeWindow(EPA input, long size) {
        this(input, size, 1L);
    }

    /**
     * @param input The input EPA of this window
     * @param size  The size of this window
     * @param slide The jump-size of this window
     */
    public TimeWindow(EPA input, long size, long slide) {
        super(input, size, slide);
    }
}
