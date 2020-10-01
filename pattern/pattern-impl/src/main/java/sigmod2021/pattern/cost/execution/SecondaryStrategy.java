package sigmod2021.pattern.cost.execution;

import sigmod2021.common.IncompatibleTypeException;
import sigmod2021.db.core.secondaryindex.SecondaryTimeIndex;
import sigmod2021.db.core.primaryindex.PrimaryIndex;
import sigmod2021.pattern.cost.cursor.PatternCandidateCursor;
import sigmod2021.pattern.cost.selection.CostEstimator;
import sigmod2021.pattern.cost.transform.TransformedPattern;
import sigmod2021.pattern.replay.ReplayPatternMatcher;
import sigmod2021.pattern.util.IntervalIntersectionCursor;
import sigmod2021.pattern.util.IntervalMergeCursor;
import sigmod2021.pattern.util.IntervalStatCursor;
import sigmod2021.pattern.util.WindowExtendCursor;
import sigmod2021.db.util.TimeInterval;
import sigmod2021.esp.ql.TranslatorException;
import sigmod2021.event.Event;
import xxl.core.cursor.MinimalCursor;
import xxl.core.cursors.Cursor;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.*;

/**
 *
 */
public class SecondaryStrategy implements ExecutionStrategy {

    final PrimaryIndex tree;
    final List<? extends SecondaryTimeIndex<?>> secondaries;
    final TransformedPattern pattern;
    final List<TimeInterval> lightweightCandidates;
    final List<TimeInterval> extendedLightweightIntervals;
    final TransformedPattern.ExecutableConfiguration config;
    final Estimates estimates;

    /**
     * Creates a new SecondaryStrategy instance
     * @param tree
     * @param secondaries
     * @param pattern
     * @param lightweightCandidates
     * @param extendedLightweightIntervals
     * @param config
     */
    public SecondaryStrategy(PrimaryIndex tree, List<? extends SecondaryTimeIndex<?>> secondaries,
                             TransformedPattern pattern, List<TimeInterval> lightweightCandidates,
                             List<TimeInterval> extendedLightweightIntervals, TransformedPattern.ExecutableConfiguration config, Estimates estimates) {
        this.tree = tree;
        this.secondaries = secondaries;
        this.pattern = pattern;
        this.lightweightCandidates = lightweightCandidates;
        this.extendedLightweightIntervals = extendedLightweightIntervals;
        this.config = config;
        this.estimates = estimates;
    }

    @Override
    public Estimates getEstimates() {
        return estimates;
    }

    @Override
    public ExecutionStrategy cloneWithNewEstimates(Estimates estimates) {
        return new SecondaryStrategy(tree, secondaries, pattern, lightweightCandidates, extendedLightweightIntervals, config,
                estimates);
    }

    @Override
    public String toString() {
        return String.format("Secondary [%d sub-patterns, %d conditions], %s, details: %s", config.getSubPatterns().size(), config.getConditions().size(), estimates, config);
    }

    private Execution createIntervalCursor() {
        return CostEstimator.TRACE ? new TracingExecution() : new SilentExecution();
    }

    public int getNumberOfIndexes() {
        return config.getConditions().size();
    }

    /**
     * @throws IncompatibleTypeException
     * @throws TranslatorException
     * @{inheritDoc}
     */
    @Override
    public Cursor<Event> execute() throws TranslatorException, IncompatibleTypeException {
        var exec = createIntervalCursor();
        ReplayPatternMatcher rpm = new ReplayPatternMatcher(tree, pattern.getDefinition());

        final Cursor<Event> c = rpm.executeMultiRegions(exec.createReplayIntervals());
        if (CostEstimator.TRACE) {
            return new MinimalCursor<>() {

                @Override
                public boolean hasNext() {
                    return c.hasNext();
                }

                @Override
                public Event next() {
                    return c.next();
                }

                @Override
                public void open() {
                    c.open();
                }

                /**
                 * @{inheritDoc}
                 */
                @Override
                public void close() {
                    c.close();
                    System.out.println(exec.traceStats("    "));
                }
            };
        } else
            return c;

    }

    /**
     * @{inheritDoc}
     */
    @Override
    public long executeIOOnly() {
        long numEvents = 0L;
        long t = -System.currentTimeMillis();
        var exec = createIntervalCursor();
        var intervals = exec.createReplayIntervals();
//		var tc = tree.query(intervals);
        try {
            intervals.open();
            t += System.currentTimeMillis();
            CostEstimator.trace("SIO took %d ms", t);
            t = -System.currentTimeMillis();
            var ic = tree.query(intervals);
            try {
                ic.open();
                while (ic.hasNext()) {
                    var c = ic.next();
                    try {
                        c.open();
                        while (c.hasNext()) {
                            numEvents++;
                            c.next();
                        }
                    } finally {
                        c.close();
                    }
                }
            } finally {
                ic.close();
            }
            t += System.currentTimeMillis();
            CostEstimator.trace("PIO took %d ms", t);
        } finally {
            intervals.close();
        }
        CostEstimator.trace(exec.traceStats("    "));

        return numEvents;
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public TransformedPattern.ExecutableConfiguration getConfig() {
        return this.config;
    }

    private static interface Execution {

        Cursor<TimeInterval> createReplayIntervals();

        String traceStats(String prefix);
    }

    private class TracingExecution implements Execution {
        Cursor<? extends TimeInterval> candidates = new PatternCandidateCursor(pattern.getWindow(), config, secondaries, lightweightCandidates);
        IntervalStatCursor candidateStats = new IntervalStatCursor(candidates);
        Cursor<TimeInterval> extended = new WindowExtendCursor(candidateStats, config, pattern.getWindow());
        IntervalStatCursor extendedStats = new IntervalStatCursor(extended);
        Cursor<TimeInterval> merged = new IntervalMergeCursor(extendedStats);
        IntervalStatCursor mergedStats = new IntervalStatCursor(merged);
        Cursor<TimeInterval> intersect = new IntervalIntersectionCursor(mergedStats, extendedLightweightIntervals.iterator());
        IntervalStatCursor intersectStats = new IntervalStatCursor(intersect);

        public Cursor<TimeInterval> createReplayIntervals() {
            return intersectStats;
        }

        /**
         * @{inheritDoc}
         */
        @Override
        public String traceStats(String prefix) {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            pw.printf("%sCandidate count                   : %8d%n", prefix, candidateStats.getIntervalCount());
            pw.printf("%sAverage candidate duration        : %8d%n", prefix, candidateStats.getAverageIntervalDuration());
            pw.printf("%sAverage replay duration           : %8d%n", prefix, extendedStats.getAverageIntervalDuration());
            pw.printf("%sMerged interval count             : %8d%n", prefix, mergedStats.getIntervalCount());
            pw.printf("%sAverage merged interval duration  : %8d%n", prefix, mergedStats.getAverageIntervalDuration());
            pw.printf("%sMerged covered timespan           : %8d%n", prefix, mergedStats.getTotalDuration());
            pw.printf("%sTrimmed interval count            : %8d%n", prefix, intersectStats.getIntervalCount());
            pw.printf("%sAverage trimmed interval duration : %8d%n", prefix, intersectStats.getAverageIntervalDuration());
            pw.printf("%sTrimmed covered timespan          : %8d%n", prefix, intersectStats.getTotalDuration());
            pw.close();
            return sw.toString();
        }
    }

    ;

    private class SilentExecution implements Execution {
        Cursor<? extends TimeInterval> candidates = new PatternCandidateCursor(pattern.getWindow(), config, secondaries, lightweightCandidates);
        Cursor<TimeInterval> extended = new WindowExtendCursor(candidates, config, pattern.getWindow());
        Cursor<TimeInterval> merged = new IntervalMergeCursor(extended);
        Cursor<TimeInterval> intersect = new IntervalIntersectionCursor(merged, extendedLightweightIntervals.iterator());

        public Cursor<TimeInterval> createReplayIntervals() {
            return intersect;
        }

        /**
         * @{inheritDoc}
         */
        @Override
        public String traceStats(String prefix) {
            return "";
        }
    }

}
