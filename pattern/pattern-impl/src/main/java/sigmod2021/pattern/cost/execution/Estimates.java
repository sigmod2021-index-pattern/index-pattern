package sigmod2021.pattern.cost.execution;

import sigmod2021.db.core.IOMeasuring;

import java.util.List;
import java.util.stream.Collectors;

public class Estimates implements Comparable<Estimates> {

    private static final double LN_2 = Math.log(2);
    private final long primaryLeafIO;
    private final long primaryNavigationIO;
    private final List<SecondaryIndexEstimate> secondary;
    private final long replayIntervals;
    private final long unmergedIntervals;
    private final double temporalCoverage;

    public Estimates(long primaryIO, long primaryNavigationIO, List<SecondaryIndexEstimate> secondaryIO, long replayIntervals, long unmergedIntervals, double temporalCoverage) {
        this.primaryLeafIO = primaryIO;
        this.primaryNavigationIO = primaryNavigationIO;
        this.secondary = secondaryIO;
        this.replayIntervals = replayIntervals;
        this.unmergedIntervals = unmergedIntervals;
        this.temporalCoverage = temporalCoverage;
    }

    long getTotalIO() {
        return primaryLeafIO + primaryNavigationIO + getSecondaryIO();
    }

    public long getScore() {
        long secondaryIO = getSecondaryIO();
        long secondarySort = secondary.stream().collect(Collectors.summarizingLong(
                x -> (long) (x.matchCount * (Math.log(x.matchCount) / LN_2))
        )).getSum();


        // Via calibration, no direct I/O
//        Weight PIO : 1.0
//        Weight SIO : 2.536732308682677
//        Weight SORT: 12.731394655606488
        double wPIO = 1.0;
        double wSIO = 2.536732308682677;
        double wSort = 12.731394655606488;

//        Weight PIO : 1.1353151294808543
//        Weight SIO : 2.826448612344557
//        Weight SORT: 19.167705654268115
//        double wPIO = 1.1353151294808543;
//        double wSIO = 2.826448612344557;
//        double wSort = 19.167705654268115;


        // Via calibration, direct I/O
        // Weight PIO : 1.0
        // Weight SIO : 1.9914566452936369
        // Weight SORT: 5.014245074757754
//        double wSIO = 1.9914566452936369;
//        double wSort = 5.014245074757754;

        long secondaryTotal = (long) Math.ceil(wSIO * secondaryIO + wSort * secondarySort);
        return primaryNavigationIO + (long) Math.ceil(wPIO * primaryLeafIO) + secondaryTotal;
    }

    @Override
    public int compareTo(Estimates o) {
        return Long.compare(getScore(), o.getScore());
    }

    boolean dominates(Estimates o) {
        return getPrimaryLeafIO() < o.getPrimaryLeafIO() && getSecondaryIO() < o.getSecondaryIO();
    }

    public long getPrimaryLeafIO() {
        return primaryLeafIO;
    }

    public long getPrimaryNavigationIO() {
        return primaryNavigationIO;
    }

    public long getSecondaryIO() {
        return secondary.stream().collect(Collectors.summarizingLong(SecondaryIndexEstimate::getIo)).getSum();
    }

    public long getReplayIntervals() {
        return replayIntervals;
    }

    public long getUnmergedIntervals() {
        return unmergedIntervals;
    }

    public double getTemporalCoverage() {
        return temporalCoverage;
    }

    public List<SecondaryIndexEstimate> getSecondary() {
        return secondary;
    }

    @Override
    public String toString() {
        return "Estimates[" +
                "primaryLeafIO=" + IOMeasuring.formatMiB(primaryLeafIO) + " MiB" +
                ", primaryNavigationIO=" + IOMeasuring.formatMiB(primaryNavigationIO) + " MiB" +
                ", secondaryIO=" + IOMeasuring.formatMiB(getSecondaryIO()) + " MiB" +
                ", replayIntervals=" + replayIntervals +
                ", unmergedIntervals=" + unmergedIntervals +
                ", temporalCoverage=" + temporalCoverage +
                ", score=" + getScore() +
                ']';
    }

    public static class SecondaryIndexEstimate {
        private final long io;
        private final long matchCount;

        public SecondaryIndexEstimate(long io, long matchCount) {
            this.io = io;
            this.matchCount = matchCount;
        }

        public long getIo() {
            return io;
        }

        public long getMatchCount() {
            return matchCount;
        }

        @Override
        public String toString() {
            return "SecondaryIndexEstimate{" +
                    "io=" + io +
                    ", matchCount=" + matchCount +
                    '}';
        }
    }
}
