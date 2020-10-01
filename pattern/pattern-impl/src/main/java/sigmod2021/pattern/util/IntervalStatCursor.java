package sigmod2021.pattern.util;

import sigmod2021.db.util.TimeInterval;
import xxl.core.cursors.AbstractCursor;
import xxl.core.cursors.Cursor;

public class IntervalStatCursor extends AbstractCursor<TimeInterval> {

    private final Cursor<? extends TimeInterval> in;

    private long intervalCount;

    private long totalDuration;

    /**
     * Creates a new IntervalStatCursor instance
     *
     * @param in
     */
    public IntervalStatCursor(Cursor<? extends TimeInterval> in) {
        this.in = in;
    }

    @Override
    public void open() {
        if (isOpened)
            return;
        super.open();
        in.open();
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public void close() {
        if (isClosed)
            return;
        super.close();
        in.close();
    }

    @Override
    protected boolean hasNextObject() {
        return in.hasNext();
    }

    @Override
    protected TimeInterval nextObject() {
        var res = in.next();
        intervalCount++;
        totalDuration += res.getDuration();
        return res;
    }

    /**
     * @return the intervalCount
     */
    public long getIntervalCount() {
        return this.intervalCount;
    }

    /**
     * @return the totalDuration
     */
    public long getTotalDuration() {
        return this.totalDuration;
    }

    public long getAverageIntervalDuration() {
        return (intervalCount == 0) ? 0 : totalDuration / intervalCount;
    }
}
