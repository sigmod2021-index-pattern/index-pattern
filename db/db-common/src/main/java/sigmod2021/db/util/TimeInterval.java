package sigmod2021.db.util;

/**
 *
 */
public class TimeInterval {

    private final long t1;

    private final long t2;

    /**
     * Creates a new TimeInterval instance
     * @param t1
     * @param t2
     */
    public TimeInterval(long t1, long t2) {
        this.t1 = t1;
        this.t2 = t2;
    }


    /**
     * @return the t1
     */
    public long getT1() {
        return this.t1;
    }


    /**
     * @return the t2
     */
    public long getT2() {
        return this.t2;
    }

    public long getDuration() {
        return t2 - t1 + 1;
    }

    /**
     * Checks if this interval intersects the given one.
     * @param other the interval to check for intersection
     * @return <code>true</code> if this interval intersects with the given one, <code>false</code> otherwise
     */
    public boolean intersects(TimeInterval other) {
        return !(t1 > other.t2 || t2 < other.t1);
    }

    /**
     * Checks if this interval is below the given one
     * @param other the interval to check
     * @return
     */
    public boolean isBelow(TimeInterval other) {
        return t2 < other.t1;
    }

    /**
     * Checks if this interval is above the given one
     * @param other the interval to check
     * @return
     */
    public boolean isAbove(TimeInterval other) {
        return t1 > other.t2;
    }

    /**
     * @param other the range to intersect this range with
     * @return the range representing the intersection of this range with the given one
     */
    public TimeInterval intersect(TimeInterval other) {
        return new TimeInterval(Math.max(t1, other.t1), Math.min(t2, other.t2));
    }

    /**
     * @param other the range to unify this range with
     * @return the range representing the union of this range with the given one
     */
    public TimeInterval union(TimeInterval other) {
        return new TimeInterval(Math.min(t1, other.t1), Math.max(t2, other.t2));
    }


    public TimeInterval adjust(long t1, long t2) {
        return new TimeInterval(t1, t2);
    }

    public boolean contains(long t) {
        return t1 <= t && t2 >= t;
    }


    /**
     * @{inheritDoc}
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int) (this.t1 ^ (this.t1 >>> 32));
        result = prime * result + (int) (this.t2 ^ (this.t2 >>> 32));
        return result;
    }


    /**
     * @{inheritDoc}
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        TimeInterval other = (TimeInterval) obj;
        if (this.t1 != other.t1)
            return false;
        if (this.t2 != other.t2)
            return false;
        return true;
    }


    /**
     * @{inheritDoc}
     */
    @Override
    public String toString() {
        return String.format("TimeInterval [t1: %s, t2: %s]", this.t1, this.t2);
    }
}
