package sigmod2021.pattern.cost.cursor;

import sigmod2021.db.core.secondaryindex.EventID;
import sigmod2021.db.core.secondaryindex.SecondaryTimeIndex;
import sigmod2021.pattern.cost.transform.TransformedPattern;
import sigmod2021.pattern.util.TIDCursor;
import sigmod2021.pattern.util.Util;
import sigmod2021.db.event.TID;
import sigmod2021.db.util.TimeInterval;
import xxl.core.cursors.AbstractCursor;
import xxl.core.cursors.Cursor;

import java.util.Iterator;
import java.util.List;

/**
 *
 */
public class SubPatternCandidateCursor extends AbstractCursor<SubPatternCandidate> {

    private final TransformedPattern.ExecutableSubPattern subPattern;

    private final List<? extends SecondaryTimeIndex<?>> indexes;
    private final EventID[] work;
    private final Cursor<EventID>[] cursors;
    private final long head;
    private final long tail;
    private List<TimeInterval> filter;
    private SubPatternCandidate nextResult;


    /**
     * Creates a new SubPatternCandidateCursor instance
     * @param subPattern
     * @param indexes
     * @param nextResult
     */
    @SuppressWarnings("unchecked")
    public SubPatternCandidateCursor(TransformedPattern.ExecutableSubPattern subPattern, List<? extends SecondaryTimeIndex<?>> indexes, List<TimeInterval> filter) {
        this.subPattern = subPattern;
        this.indexes = indexes;
        this.filter = filter;
        this.cursors = new Cursor[subPattern.getConditions().size()];
        this.work = new EventID[subPattern.getConditions().size()];
        this.head = subPattern.getConditions().get(0).getId().absolutePosition;
        this.tail = subPattern.getLength() - 1 - subPattern.getConditions().get(subPattern.getConditions().size() - 1).getId().absolutePosition;
    }

    /**
     * @{inheritDoc}
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    @Override
    public void open() {
        if (isOpened)
            return;

        super.open();

        for (int i = 0; i < cursors.length; i++) {
            var c = subPattern.getConditions().get(i);
            SecondaryTimeIndex idx = Util.getIndexForRange(c.getRange(), indexes);
            cursors[i] = new TIDCursor(idx.rangeQueryID(c.getRange().getLower(), c.getRange().getUpper()));

            // Apply filter
            if (filter != null && !filter.isEmpty()) {
                cursors[i] = new IntervalFilterCursor(filter.iterator(), cursors[i]);
            }

            cursors[i].open();
        }
        // TODO
        for (int i = 0; i < cursors.length; i++) {
            // Prefill work
            if (i > 0 && !cursors[i].hasNext())
                return;
            else
                work[i] = cursors[i].next();

        }
        nextResult = computeNextResult();
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public void close() {
        if (isClosed)
            return;

        super.close();

        for (var c : cursors)
            c.close();
    }

    private SubPatternCandidate computeNextResult() {
        SubPatternCandidate result = null;

        outer:
        while (cursors[0].hasNext() && result == null) {
            work[0] = cursors[0].next();

            for (int i = 1; i < subPattern.getConditions().size(); i++) {
                var condition = subPattern.getConditions().get(i);

                final long seqPrev = work[i - 1].getSequenceId();
                long seqMin = seqPrev + condition.getMinDist();

                nextElement(i, seqMin);

                // Break - no further results possible
                if (work[i] == null)
                    return null;

                final long seqDelta = work[i].getSequenceId() - seqPrev;

                // Minimum is ensured via #next(idx,seqMin)
                if (seqDelta > condition.getMaxDist())
                    continue outer;
            }

            // Construct result
            result = new SubPatternCandidate(
                    new TID(work[0].getBlockId(), work[0].getOffset()),
                    work[0].getTimestamp(),
                    work[work.length - 1].getTimestamp(),
                    // Sequence stuff
                    work[0].getSequenceId() - head,
                    work[work.length - 1].getSequenceId() + tail);
        }
        return result;
    }

    private void nextElement(int idx, long seqMin) {
        while (work[idx].getSequenceId() < seqMin && cursors[idx].hasNext())
            work[idx] = cursors[idx].next();

        // End of Stream
        if (work[idx].getSequenceId() < seqMin)
            work[idx] = null;
    }

    /**
     * @{inheritDoc}
     */
    @Override
    protected boolean hasNextObject() {
        return nextResult != null;
    }

    /**
     * @{inheritDoc}
     */
    @Override
    protected SubPatternCandidate nextObject() {
        var res = nextResult;
        nextResult = computeNextResult();
        return res;
    }


    private class IntervalFilterCursor extends AbstractCursor<EventID> {
        private Iterator<TimeInterval> filter;
        private Cursor<EventID> data;

        private EventID nextElement;
        private TimeInterval currentFilter;

        /**
         * Creates a new IntervalFilterCursor instance
         * @param filter
         * @param data
         */
        public IntervalFilterCursor(Iterator<TimeInterval> filter, Cursor<EventID> data) {
            this.filter = filter;
            this.data = data;
        }

        /**
         * @{inheritDoc}
         */
        @Override
        public void open() {
            if (isOpened)
                return;

            super.open();
            data.open();

            if (filter.hasNext())
                currentFilter = filter.next();

            nextElement = computeNext();
        }

        private EventID computeNext() {
            EventID result = null;
            while (result == null && data.hasNext() && currentFilter != null) {
                result = data.next();
                while (currentFilter != null && result.getTimestamp() > currentFilter.getT2()) {
                    currentFilter = filter.hasNext() ? filter.next() : null;
                }

                if (currentFilter != null && !currentFilter.contains(result.getTimestamp()))
                    result = null;
            }
            return result;
        }

        /**
         * @{inheritDoc}
         */
        @Override
        public void close() {
            if (isClosed)
                return;

            super.close();
            data.close();
        }


        /**
         * @{inheritDoc}
         */
        @Override
        protected boolean hasNextObject() {
            return nextElement != null;
        }

        /**
         * @{inheritDoc}
         */
        @Override
        protected EventID nextObject() {
            var res = nextElement;
            nextElement = computeNext();
            return res;
        }
    }

}
