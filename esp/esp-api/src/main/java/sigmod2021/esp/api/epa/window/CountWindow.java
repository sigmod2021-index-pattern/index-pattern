package sigmod2021.esp.api.epa.window;

import sigmod2021.esp.api.epa.EPA;

/**
 * Representation of a count based window EPA.
 * This means that the window always holds N events and jumps
 * JUMP units on each update.
 */
public class CountWindow extends Window {

    private static final long serialVersionUID = 1L;

    /**
     * @param input The input EPA of this window
     * @param size  The size of this window
     */
    public CountWindow(EPA input, long size) {
        this(input, size, 1L);
    }

    /**
     * @param input The input EPA of this window
     * @param size  The size of this window
     * @param jump  The jump-size of this window
     */
    public CountWindow(EPA input, long size, long jump) {
        super(input, size, jump);
    }

}
