package sigmod2021.esp.bridges.nat.epa.impl.aggregator;

import sigmod2021.esp.api.bridge.EPACallback;
import sigmod2021.esp.api.bridge.EventChannel;
import sigmod2021.esp.api.epa.Aggregator;
import sigmod2021.esp.api.epa.aggregates.Aggregate;
import sigmod2021.esp.api.epa.aggregates.Group;
import sigmod2021.esp.api.util.EventPartitioner;
import sigmod2021.esp.bridges.nat.epa.NativeOperator;
import sigmod2021.esp.bridges.nat.epa.impl.aggregator.aggregates.AggregateFunction;
import sigmod2021.esp.bridges.nat.epa.impl.aggregator.aggregates.AggregateTranslator;
import sigmod2021.esp.bridges.nat.epa.util.AscTimestampComparator;
import sigmod2021.event.Event;
import sigmod2021.event.EventSchema;
import sigmod2021.event.SchemaException;
import sigmod2021.util.Pair;

import java.io.Serializable;
import java.util.*;

/**
 * Implements an Aggregator by using an Agg-2-3-tree per group.
 */
public class NativeAggregator implements NativeOperator, EPACallback {

    /**
     * The definition
     */
    private final Aggregator def;

    /**
     * The output-schema of this Aggregator
     */
    private final EventSchema outputSchema;

    private final EventPartitioner partitioner;

    /**
     * Holds the output-position of group aggregates
     */
    private final int[] groupInputIndices;

    /**
     * Holds the output-position of group aggregates
     */
    private final int[] groupOutputIndices;

    /**
     * Holds all aggregate-functions to compute
     */
    private final AggregateFunctionHolder[] aggregates;
    /**
     * Keeps Time2Group records ordered by timestamps.
     */
    private final TreeSet<Time2Groups> time2Groups;
    /**
     * Has exactly the same content as time2Groups, but allows efficient
     * look-ups for a given timestamp.
     */
    private final Map<Long, Time2Groups> time2GroupsMap;
    /**
     * Stores for each group its corresponding Agg-2-3-tree
     */
    private final Map<Object, A23Tree> group2tree;
    /**
     * Output buffer needed to ensure timely correct order of output stream.
     */
    private final PriorityQueue<Event> outputBuffer;
    /**
     * The clock of this aggregator
     */
    private long minTSIn = Long.MIN_VALUE;
    /**
     * Reference to the tree with the minimal output start-timestamp
     */
    private A23Tree minTSTree = null;

    /**
     * Holds the minimum timestamp on which results may occur
     */
    private long minTS = Long.MAX_VALUE;

    /**
     * The callback which receives computed aggregates
     */
    private EPACallback callback;

    /**
     * Constructs a new NativeAggregator-instance
     *
     * @param agg         the aggregate definition
     * @param inputSchema the schema of incoming events
     * @param callback    the output-receiver
     * @throws SchemaException
     */
    public NativeAggregator(Aggregator agg, EventChannel input) throws SchemaException {
        this.def = agg;
        this.minTSIn = -1;
        this.time2Groups = new TreeSet<>();
        this.time2GroupsMap = new HashMap<>();
        this.group2tree = new LinkedHashMap<Object, A23Tree>(16, 0.75f, true);
        this.outputSchema = agg.computeOutputSchema(input.getSchema());
        this.outputBuffer = new PriorityQueue<>(new AscTimestampComparator());

        // Create aggregate-functions
        List<AggregateFunctionHolder> aggregates = new ArrayList<>();
        List<Pair<Integer, Integer>> groups = new ArrayList<>();

        // Collect groups and aggregates
        int i = 0;
        for (Aggregate a : def.getAggregates()) {
            int inputPos = input.getSchema().getAttributeIndex(a.getAttributeIn());
            if (a instanceof Group)
                groups.add(new Pair<>(inputPos, i));
            else
                aggregates.add(new AggregateFunctionHolder(inputPos, i,
                        AggregateTranslator.translateAggregate(a, input.getSchema())));
            i++;
        }

        // Store group keys
        this.groupInputIndices = new int[groups.size()];
        this.groupOutputIndices = new int[groups.size()];
        for (i = 0; i < this.groupInputIndices.length; i++) {
            this.groupInputIndices[i] = groups.get(i)._1;
            this.groupOutputIndices[i] = groups.get(i)._2;
        }
        this.aggregates = aggregates.toArray(new AggregateFunctionHolder[aggregates.size()]);
        this.partitioner = new EventPartitioner(this.groupInputIndices);
    }

    @Override
    public EventSchema getSchema() {
        return outputSchema;
    }

    @Override
    public void setCallback(EPACallback callback) {
        this.callback = callback;
    }

    @Override
    public void process(EventChannel input, Event event) {
        long tstart = event.getT1();
        long tend = event.getT2();
        boolean flush = false;

        // Set global update -- collect results
        if (tstart > minTSIn) {
            minTSIn = tstart;
            flush |= collectResults(tstart);
        }

        // group ID
        Object partitionId = partitioner.buildKey(event);

        // Get tree
        A23Tree tree = group2tree.get(partitionId);
        if (tree == null) {
            tree = createTree(partitionId, event);
            group2tree.put(partitionId, tree);
        }

        // 1) Flush our tree if we have a partial aggregate
        if (tree.getLastUpdate() < tstart) {
            flush |= (tree == minTSTree);
            tree.forwardPendingResults(tstart);
        }

        // 2) Insert new aggregate
        tree.insert(event);

        // 3) Release output buffer
        if (flush)
            pushOutputEvents();

        // 4) Update time2groups
        Time2Groups t2g = time2GroupsMap.get(tend);
        if (t2g == null) {
            t2g = new Time2Groups(tend);
            time2Groups.add(t2g);
            time2GroupsMap.put(tend, t2g);
        }
        t2g.groups.add(tree);
    }


    /**
     * @{inheritDoc}
     */
    @Override
    public void flushState() {
        // Flush tree
        collectResults(Long.MAX_VALUE);

        // Output results
        while (!outputBuffer.isEmpty()) {
            callback.receive(outputBuffer.poll());
        }
    }


    @Override
    public void receive(Event event) {
        if (event.getT1() == minTS)
            callback.receive(event);
        else
            outputBuffer.add(event);
    }

    /**
     * Releases output events in a timely correct order.
     */
    private void pushOutputEvents() {

        // Minimum always on first position
        if (!group2tree.isEmpty()) {
            minTSTree = group2tree.values().iterator().next();
            minTS = minTSTree.getLastUpdate();
        } else {
            minTSTree = null;
            minTS = Long.MAX_VALUE;
        }

        while (!outputBuffer.isEmpty() && outputBuffer.peek().getT1() <= minTS)
            callback.receive(outputBuffer.poll());
    }

    /**
     * Creates a new Agg-2-3-Tree.
     *
     * @param groupPattern the pattern to identify the tree
     * @param event        the first event triggered the tree-creation
     * @return the new tree to insert the event into
     */
    private A23Tree createTree(final Object partitionId, final Event event) {
        // Create std-template
        Object[] skeleton = new Object[outputSchema.getNumAttributes()];
        for (AggregateFunctionHolder h : aggregates) {
            skeleton[h.outputIndex] = h.aggregate.fInit();
        }
        // Set groups
        for (int i = 0; i < groupInputIndices.length; i++) {
            skeleton[groupOutputIndices[i]] = event.get(groupInputIndices[i]);
        }

        A23Tree res = new A23Tree(partitionId, aggregates, skeleton, this);

        // Initial value = first tree
        if (minTSTree == null) {
            minTSTree = res;
            minTS = event.getT1();
        }
        return res;
    }

    /**
     * Flushes all pending results unti the given timestamp (inclusive)
     *
     * @param until the timestamp until which the results are flushed
     * @return true if the output-buffer should be flushed, false otherwise
     */
    public boolean collectResults(final long until) {
        boolean result = false;
        Time2Groups t2g;

        while (!time2Groups.isEmpty() && time2Groups.first().t <= until) {
            t2g = time2Groups.pollFirst();
            time2GroupsMap.remove(t2g.t);

            // Find all expired partial aggregates
            for (A23Tree t : t2g.groups) {
                t.forwardResults(t2g.t);
                // New minimum?
                result |= t == minTSTree;
                // Remove empty trees
                if (t.isEmpty())
                    group2tree.remove(t.getPartitionId());
                    // Maintain ordering in LinkedHashMap !!! DO NOT REMOVE !!!
                else
                    group2tree.get(t.getPartitionId());
            }
        }
        return result;
    }

    /**
     * Flushes all events until the given timestamp
     *
     * @param until the timestamp to flush results to
     */
    public void computeExpiredEvents(long until) {
        System.out.println("computeExpiredEvents " + until);
        minTSIn = until;
        if (collectResults(until)) {
            pushOutputEvents();
        }
    }

    public long getLastUpdate() {
        return minTSIn;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Inner classes //
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    /**
     * Time2Group records.
     */
    private static class Time2Groups implements Comparable<Time2Groups>, Serializable {

        private static final long serialVersionUID = 1L;

        // timestamp used as key
        public final long t;
        // set of groups
        public final Set<A23Tree> groups;

        public Time2Groups(long tend) {
            t = tend;
            groups = new HashSet<>();
        }

        @Override
        public int compareTo(Time2Groups o) {
            return Long.compare(t, o.t);
        }
    }

    /**
     * Holds aggregate functions and their working indices
     */
    public static class AggregateFunctionHolder implements Serializable {

        private static final long serialVersionUID = 1L;

        final int inputIndex;
        final int outputIndex;
        @SuppressWarnings("rawtypes")
        final AggregateFunction aggregate;

        public AggregateFunctionHolder(int inputIndex, int outputIndex, AggregateFunction<?, ?, ?> aggregate) {
            this.inputIndex = inputIndex;
            this.outputIndex = outputIndex;
            this.aggregate = aggregate;
        }
    }
}
