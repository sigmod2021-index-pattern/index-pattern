package sigmod2021.esp.bridges.nat.epa.impl.window;

import sigmod2021.esp.api.bridge.EPACallback;
import sigmod2021.esp.api.bridge.EventChannel;
import sigmod2021.esp.api.epa.window.PartitionedCountWindow;
import sigmod2021.esp.bridges.nat.epa.util.AscTimestampComparator;
import sigmod2021.event.Event;
import sigmod2021.event.EventSchema;
import sigmod2021.event.SchemaException;

import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;

/**
 * Implements a partitioned count-based window
 */
public class NativePartitionedCountWindow extends NativeWindow implements EPACallback {

    /**
     * Separator for building a unique partition identifier
     */
    private static final char HASH_SEP = ':';

    /**
     * The window definition
     */
    private final PartitionedCountWindow def;

    /**
     * Stores the positions of all attributes that are used to partition the input
     * event stream.
     */
    private final int[] partitionAttributes;

    /**
     * Stores all active partitions.
     */
    private final Map<Object, NativeCountWindow> partitions;

    /**
     * Buffers the output of partitioned count-based windows. Only events with a
     * timestamp that all active partitions have reached are safe to be reported!
     */
    private final PriorityQueue<Event> partitionedCountBuffer;

    /**
     * @param input the input channel
     * @param def   the window definition
     * @throws SchemaException if a defined paritioning attribute is not present in the input stream
     */
    public NativePartitionedCountWindow(EventChannel input, PartitionedCountWindow def) throws SchemaException {
        super(input, def);
        this.def = def;
        this.partitionAttributes = computPartitionAttributes(input.getSchema(), def.getPartitionAttributes());
        partitions = new HashMap<>();
        partitionedCountBuffer = new PriorityQueue<>(128, new AscTimestampComparator());
    }

    /**
     * Computes the positions of the given partition attributes
     *
     * @param schema      the schema of incoming events
     * @param partitionBy the partition attributes
     * @return the positions of the given partition attributes on incoming events
     * @throws SchemaException
     */
    private static int[] computPartitionAttributes(EventSchema schema, String[] partitionBy) throws SchemaException {
        int[] partitionAttributes = new int[partitionBy.length];
        for (int i = 0; i < partitionAttributes.length; i++) {
            partitionAttributes[i] = schema.getAttributeIndex(partitionBy[i]);
        }
        return partitionAttributes;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void process(EventChannel input, Event event) {
        Object hash;
        if (partitionAttributes.length == 1) {
            hash = event.get(partitionAttributes[0]);
        } else {
            StringBuilder sb = new StringBuilder();
            for (int idx : partitionAttributes) {
                sb.append(event.get(idx)).append(HASH_SEP);
            }
            hash = sb.toString();
        }

        NativeCountWindow partition = partitions.get(hash);
        if (partition == null) {
            partition = new NativeCountWindow(input, def);
            partition.setCallback(this);
            partitions.put(hash, partition);
        }
        partition.process(input, event);
        flushInternal();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void receive(Event event) {
        partitionedCountBuffer.add(event);
    }

    /**
     * Flushes the output-heap to the actual stream-time
     */
    private void flushInternal() {
        long minTimestamp = getMinTimestamp(); // TODO Splits wenn eine Partition hängt!!
        while (!partitionedCountBuffer.isEmpty() && partitionedCountBuffer.peek().getT1() <= minTimestamp) {
            callback.receive(partitionedCountBuffer.poll());
        }
    }

    /**
     * Gets the minimum clock among all input streams.
     *
     * @return minimum clock among all input streams
     */
    private long getMinTimestamp() {
        long minTimestamp = Long.MAX_VALUE;
        for (NativeCountWindow w : partitions.values())
            minTimestamp = Math.min(minTimestamp, w.tstart);
        return minTimestamp;
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public void flushState() {
        for (NativeCountWindow w : partitions.values()) {
            w.flushState();
        }
        while (!partitionedCountBuffer.isEmpty()) {
            callback.receive(partitionedCountBuffer.poll());
        }
    }
}
