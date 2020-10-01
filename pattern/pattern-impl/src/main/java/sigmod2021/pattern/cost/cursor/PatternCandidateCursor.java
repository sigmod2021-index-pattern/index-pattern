package sigmod2021.pattern.cost.cursor;

import sigmod2021.db.core.secondaryindex.SecondaryTimeIndex;
import sigmod2021.pattern.cost.transform.TransformedPattern;
import sigmod2021.db.util.TimeInterval;
import xxl.core.cursors.AbstractCursor;
import xxl.core.cursors.Cursor;

import java.util.Collections;
import java.util.List;


/**
 *
 */
public class PatternCandidateCursor extends AbstractCursor<TimeInterval> {

    private final long window;

    private final List<TimeInterval> filter;

    private final List<TransformedPattern.ExecutableSubPattern> subPatterns;

    private final List<? extends SecondaryTimeIndex<?>> indexes;

    private final SubPatternCandidate[] work;

    private final Cursor<SubPatternCandidate>[] cursors;

    private TimeInterval nextResult = null;

    /**
     * Creates a new CandidateCursor instance
     */
    public PatternCandidateCursor(long window, TransformedPattern.ExecutableConfiguration config, List<? extends SecondaryTimeIndex<?>> indexes) {
        this(window, config, indexes, Collections.emptyList());
    }

    /**
     * Creates a new CandidateCursor instance
     */
    @SuppressWarnings("unchecked")
    public PatternCandidateCursor(long window, TransformedPattern.ExecutableConfiguration config, List<? extends SecondaryTimeIndex<?>> indexes, List<TimeInterval> filter) {
        this.window = window;
        this.subPatterns = config.getSubPatterns();
        this.indexes = indexes;
        this.work = new SubPatternCandidate[subPatterns.size()];
        this.cursors = new Cursor[subPatterns.size()];
        this.filter = filter;
    }


    /**
     * @{inheritDoc}
     */
    @Override
    public void open() {
        if (isOpened)
            return;

        super.open();

        for (int i = 0; i < cursors.length; i++) {
            var c = subPatterns.get(i);
            cursors[i] = new SubPatternCandidateCursor(c, indexes, filter);
            cursors[i].open();
        }
        for (int i = 0; i < cursors.length; i++) {
            // Prefill work
            if (!cursors[i].hasNext())
                return;
            else if (i > 0)
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


    TimeInterval computeNextResult() {
        TimeInterval result = null;

        while (cursors[0].hasNext() && result == null) {
            work[0] = cursors[0].next();
            for (int i = 1; i < subPatterns.size(); i++) {

                nextElement(i, work[i - 1].getMaxSeq() + 1);

                // Break - no further results possible
                if (work[i] == null)
                    return null;
            }

            // Check window condition
            long ts = work[0].getMinTs();
            long te = work[work.length - 1].getMaxTs();

            if (te - ts <= window)
                result = new TimeInterval(ts, te);
        }
        return result;
    }

    private void nextElement(int idx, long seqMin) {
        while (work[idx].getMinSeq() < seqMin && cursors[idx].hasNext())
            work[idx] = cursors[idx].next();

        // End of Stream
        if (work[idx].getMinSeq() < seqMin)
            work[idx] = null;
    }


    @Override
    protected boolean hasNextObject() {
        return nextResult != null;
    }

    @Override
    protected TimeInterval nextObject() {
        var res = nextResult;
        nextResult = computeNextResult();
        return res;
    }


}
