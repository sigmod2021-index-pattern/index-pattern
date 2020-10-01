package sigmod2021.pattern.util;

import sigmod2021.db.util.TimeInterval;
import xxl.core.cursors.AbstractCursor;
import xxl.core.cursors.Cursor;
import xxl.core.cursors.Cursors;

import java.util.Arrays;
import java.util.Iterator;


/**
 * Cursor intersecting two interval streams. This cursor assumes,
 * that intervals in each input are disjoint.
 *
 */
public class IntervalIntersectionCursor extends AbstractCursor<TimeInterval> {

    private final Cursor<TimeInterval> in1, in2;

    private TimeInterval current1, current2, next;

    /**
     * Creates a new IntervalIntersectionCursor instance
     * @param in1
     * @param in2
     */
    public IntervalIntersectionCursor(Iterator<TimeInterval> in1, Iterator<TimeInterval> in2) {
        this.in1 = Cursors.wrap(in1);
        this.in2 = Cursors.wrap(in2);
    }

    public static void main(String[] args) {
        var in1 = Arrays.asList(new TimeInterval(5, 15)).iterator();
        var in2 = Arrays.asList(new TimeInterval(10, 15)).iterator();

        new IntervalIntersectionCursor(in1, in2).forEachRemaining(System.out::println);

        System.out.println("================================");

        in1 = Arrays.asList(new TimeInterval(5, 10), new TimeInterval(22, 25), new TimeInterval(27, 30)).iterator();
        in2 = Arrays.asList(new TimeInterval(4, 8), new TimeInterval(20, 28)).iterator();

        new IntervalIntersectionCursor(in1, in2).forEachRemaining(System.out::println);
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public void open() {
        if (isOpened)
            return;

        super.open();
        in1.open();
        in2.open();

        if (in1.hasNext())
            current1 = in1.next();
        if (in2.hasNext())
            current2 = in2.next();

        next = computeNext();
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public void close() {
        super.close();
    }

    private TimeInterval computeNext() {
        TimeInterval result = null;

        while (current1 != null && current2 != null && result == null) {
            if (current1.intersects(current2)) {
                result = current1.intersect(current2);
            }
            // Forward 2nd input
            if (current1.getT2() >= current2.getT2()) {
                current2 = in2.hasNext() ? in2.next() : null;
            } else {
                current1 = in1.hasNext() ? in1.next() : null;
            }
        }
        return result;
    }

    /**
     * @{inheritDoc}
     */
    @Override
    protected boolean hasNextObject() {
        return next != null;
    }

    /**
     * @{inheritDoc}
     */
    @Override
    protected TimeInterval nextObject() {
        var res = next;
        next = computeNext();
        return res;
    }

}
