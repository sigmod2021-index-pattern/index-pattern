package sigmod2021.esp.api.util;

import sigmod2021.esp.api.provider.EventSink;
import sigmod2021.event.Event;

import java.io.PrintStream;

/**
 * A sink printing each event to the given {@link PrintStream}
 */
public class PrintingSink implements EventSink {

    /**
     * The stream to print to
     */
    private final PrintStream stream;

    /**
     * Creates a new instance using {@link System#out} as stream
     */
    public PrintingSink() {
        this(System.out);
    }

    /**
     * Creates a new instance
     *
     * @param stream the stream to print events to
     */
    public PrintingSink(PrintStream stream) {
        this.stream = stream;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void receiveEvent(Event event) {
        stream.println(event.toString());
    }

}
