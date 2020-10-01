package sigmod2021.db.core.primaryindex.impl;

import sigmod2021.db.event.PersistentEvent;
import sigmod2021.db.event.TID;
import sigmod2021.db.util.TimeInterval;
import sigmod2021.event.Event;
import xxl.core.cursor.MinimalCursor;
import xxl.core.cursors.Cursor;
import xxl.core.indexStructures.BPlusTree.IndexEntry;
import xxl.core.indexStructures.BPlusTree.Node;
import xxl.core.util.Pair;
import xxl.core.util.Triple;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.NoSuchElementException;

/**
 *
 */
public class MultiIntervalCursor implements MinimalCursor<Cursor<PersistentEvent>> {

    private final Cursor<TimeInterval> intervals;
    private final long minTs;
    private final Deque<Triple<Integer, IndexEntry, Node>> path = new ArrayDeque<>();
    private long intervalCount = 0L;
    private Cursor<PersistentEvent> next;

    /**
     * Creates a new MultiIntervalCursor instance
     *
     * @param intervals
     * @param minTs
     * @param root
     */
    public MultiIntervalCursor(Cursor<TimeInterval> intervals, long minTs, Pair<? extends IndexEntry, ? extends Node> root) {
        this.intervals = intervals;
        this.minTs = minTs;
        this.path.push(new Triple<>(0, root.getElement1(), root.getElement2()));
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public void open() {
        intervals.open();
        next = computeNext();
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public void close() {
        while (!path.isEmpty())
            path.pop().getElement2().unfix();
//		System.err.println("Processed: " + intervalCount + " intervals.");
        intervals.close();
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public boolean hasNext() {
        return next != null;
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public Cursor<PersistentEvent> next() {
        var result = next;
        next = computeNext();
        return result;
    }

    private Triple<Integer, IndexEntry, Node> seekInterval(TimeInterval interval) {
        while (!path.isEmpty() && ((Long) path.peek().getElement2().separator().sepValue() < interval.getT1())) {
            path.pop().getElement2().unfix();
        }

        // Go down
        Triple<Integer, IndexEntry, Node> current = path.peek();

        if (current.getElement3().level() == 0) {
            return new Triple<>(current.getElement1(), current.getElement2(), current.getElement3());
        }

        do {
            int idx = current.getElement1();
            IndexEntry next = null;
            while (idx < current.getElement3().number() &&
                    (Long) (next = (IndexEntry) current.getElement3().getEntry(idx)).separator().sepValue() < interval
                            .getT1()) {
                idx++;
            }

            // Go to next node
            if (idx >= current.getElement3().number()) {
                path.pop().getElement2().unfix();
                current = path.isEmpty() ? null : path.peek();
            } else {
                current.setElement1(idx);

                // Next level
                current = new Triple<>(0, next, (Node) next.get(false));
                path.push(current);
            }
        } while (current != null && current.getElement3().level() > 0);

        return current == null ? null : new Triple<>(current.getElement1(), current.getElement2(), current.getElement3());

    }

    private Cursor<PersistentEvent> computeNext() {
        if (!intervals.hasNext())
            return null;

        final TimeInterval interval = intervals.next();
        final var current = seekInterval(interval);

        if (current == null) {
            return null;
        }

        return new MinimalCursor<>() {

            PersistentEvent next;

            @Override
            public void open() {
                while ((next = computeNext()) != null && next.getT1() < interval.getT1()) ;
            }

            @Override
            public void close() {
                current.getElement2().unfix();
            }

            private PersistentEvent computeNext() {
                int idx = current.getElement1();
                if (idx >= current.getElement3().number()) {
                    IndexEntry nn = current.getElement3().nextNeighbor;

                    current.getElement2().unfix();

                    if (nn == null)
                        return null;

                    idx = 0;
                    current.setElement2(nn);
                    current.setElement3((Node) nn.get(false));
                }

                Event tmp = (Event) current.getElement3().getEntry(idx);
                current.setElement1(idx + 1);

                if (tmp.getT1() <= interval.getT2())
                    return new PersistentEvent(new TID((Long) current.getElement2().id(), idx), tmp);
                else
                    return null;
            }


            @Override
            public boolean hasNext() throws IllegalStateException {
                return next != null;
            }

            @Override
            public PersistentEvent next() throws IllegalStateException, NoSuchElementException {
                PersistentEvent result = next;
                next = computeNext();
                return result;
            }
        };
    }
}
