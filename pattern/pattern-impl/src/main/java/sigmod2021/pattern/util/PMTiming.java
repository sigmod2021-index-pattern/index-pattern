package sigmod2021.pattern.util;

import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class PMTiming {

    public static final boolean DO_TIMING = true;

    private static final List<PMTiming> timings = new ArrayList<PMTiming>();

    private static PMTiming currentTiming = new PMTiming();
    private long secondaryReadNanos = 0L;
    private long secondarySortNanos = 0L;
    private long totalNanos = 0L;
    private long lightweightNanos = 0L;
    private long selectionNanos = 0L;

    public static PMTiming getCurrentTiming() {
        return currentTiming;
    }

    public static void nextTiming() {
        timings.add(currentTiming);
        currentTiming = new PMTiming();
    }

    public static List<PMTiming> finish() {
        timings.add(currentTiming);
        List<PMTiming> result = new ArrayList<>(timings);
        timings.clear();
        return result;
    }

    public static void start() {
        currentTiming = new PMTiming();
        timings.clear();
    }

    public void addSecondaryReadNanos(long nanos) {
        this.secondaryReadNanos += nanos;
    }

    public void addSecondarySortNanos(long nanos) {
        this.secondarySortNanos += nanos;
    }

    /**
     * @return the secondaryRead
     */
    public long getSecondaryReadNanos() {
        return this.secondaryReadNanos;
    }

    /**
     * @return the secondarySort
     */
    public long getSecondarySortNanos() {
        return this.secondarySortNanos;
    }

    /**
     * @return the totalNanos
     */
    public long getTotalNanos() {
        return this.totalNanos;
    }

    public void setTotalNanos(long nanos) {
        this.totalNanos = nanos;
    }

    /**
     * @return the lightweightNanos
     */
    public long getLightweightNanos() {
        return this.lightweightNanos;
    }

    public void setLightweightNanos(long nanos) {
        this.lightweightNanos = nanos;
    }

    /**
     * @return the selectionNanos
     */
    public long getSelectionNanos() {
        return this.selectionNanos;
    }


    /**
     * @param selectionNanos the selectionNanos to set
     */
    public void setSelectionNanos(long selectionNanos) {
        this.selectionNanos = selectionNanos;
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public String toString() {
        return String.format(
                "PMTiming [secondaryReadNanos: %s, secondarySortNanos: %s, totalNanos: %s, lightweightNanos: %s, selectionNanos: %s]",
                this.secondaryReadNanos, this.secondarySortNanos, this.totalNanos, this.lightweightNanos,
                this.selectionNanos);
    }
}
