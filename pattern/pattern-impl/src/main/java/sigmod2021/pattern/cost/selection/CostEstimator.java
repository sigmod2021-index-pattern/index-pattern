package sigmod2021.pattern.cost.selection;

import org.apache.commons.math3.analysis.integration.BaseAbstractUnivariateIntegrator;
import org.apache.commons.math3.analysis.integration.RombergIntegrator;
import org.apache.commons.math3.analysis.integration.UnivariateIntegrator;
import org.apache.commons.math3.distribution.AbstractRealDistribution;
import org.apache.commons.math3.distribution.ExponentialDistribution;
import org.apache.commons.math3.distribution.RealDistribution;
import sigmod2021.db.core.secondaryindex.SecondaryTimeIndex;
import sigmod2021.db.core.primaryindex.impl.PrimaryIndexImpl;
import sigmod2021.pattern.cost.estimation.prob.HypoExponentialDistribution;
import sigmod2021.pattern.cost.execution.Estimates;
import sigmod2021.pattern.cost.execution.HistogramOnlyStrategy;
import sigmod2021.pattern.cost.execution.SecondaryStrategy;
import sigmod2021.pattern.cost.selection.PatternStats.ConditionStats;
import sigmod2021.pattern.cost.transform.SubPatternCondition;
import sigmod2021.pattern.cost.transform.TransformedPattern;
import sigmod2021.pattern.util.IntervalMergeCursor;
import sigmod2021.pattern.util.Util;
import sigmod2021.db.util.TimeInterval;
import xxl.core.cursors.Cursor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 *
 */
public class CostEstimator {

    public static final boolean TRACE = false;

    private final PrimaryIndexImpl tree;

    private final List<? extends SecondaryTimeIndex<?>> secondaries;

    private final TransformedPattern pattern;

    private final List<SelSubPattern> selectivityEnrichedSubPatterns;

    private final double searchSpace;

    private final double extendedSearchSpace;

    private final double numEvents;

    private final double globalEventCount;

    // Filter for secondary indexes
    private final List<TimeInterval> lightweightCandidates = new ArrayList<>();
    // Filter for replay intervals
    private final List<TimeInterval> extendedLightweightIntervals = new ArrayList<>();

    private final Map<SubPatternCondition.ConditionId, ConditionStats> statsMap = new HashMap<>();


    private final UnivariateIntegrator integrator;

    public CostEstimator(PrimaryIndexImpl primary, List<? extends SecondaryTimeIndex<?>> secondaries,
                         TransformedPattern pattern, PatternStats result) {
        this.tree = primary;
        this.secondaries = secondaries;
        this.pattern = pattern;
        this.integrator = new RombergIntegrator(
                BaseAbstractUnivariateIntegrator.DEFAULT_RELATIVE_ACCURACY * 10000,
                BaseAbstractUnivariateIntegrator.DEFAULT_ABSOLUTE_ACCURACY,
                BaseAbstractUnivariateIntegrator.DEFAULT_MIN_ITERATIONS_COUNT,
                RombergIntegrator.ROMBERG_MAX_ITERATIONS_COUNT);

        var csi = result.getConditionStats().iterator();
        for (var c : pattern.collectConditions()) {
            statsMap.put(c.getId(), csi.next());
        }

        selectivityEnrichedSubPatterns =
                pattern.getSubPatterns().stream().map(sp -> new SelSubPattern(sp, statsMap)).collect(Collectors.toList());


        long duration = 0;
        long eventCount = 0;
        List<TimeInterval> tmpELI = new ArrayList<>();
        for (var mc : result.getMatchIntervals()) {
            lightweightCandidates.add(mc.getInterval());
            tmpELI.add(pattern.getLightweightTScope(mc));
            eventCount += mc.getEventCount();
            duration += mc.getInterval().getDuration();
        }

        Cursor<TimeInterval> intervals = new IntervalMergeCursor<>(tmpELI.iterator());
        intervals.open();
        long extendedDuration = 0L;
        while (intervals.hasNext()) {
            var i = intervals.next();
            extendedLightweightIntervals.add(i);
            extendedDuration += i.getDuration();
        }
        intervals.close();

        searchSpace = duration;
        extendedSearchSpace = extendedDuration;
        numEvents = eventCount;
        globalEventCount = tree.getNumberOfEvents();
    }

    private static long estimateCoveredTimespan(double l, double L, long unmergedIntervalCount) {
        double probOverlap = Math.min(l / L, 1.0);
        double pCovered = 1.0 - Math.pow((1.0 - probOverlap), unmergedIntervalCount);
        return (long) Math.ceil(L * pCovered);
    }

    private static long estimateMergedIntervalCount(double l, double L, long unmergedIntervalCount) {
        double probOverlap = Math.min(l / L, 1.0);
        double pUncovered = Math.pow((1.0 - Math.min(probOverlap, 1.0)), unmergedIntervalCount);
        return (long) Math.max(1.0, Math.ceil(unmergedIntervalCount * pUncovered));
    }

    public static void trace(String format, Object... args) {
        if (TRACE)
            System.out.printf(format, args);
    }

    public List<SelSubPattern> getSelectivityEnrichedSubPatterns() {
        return selectivityEnrichedSubPatterns;
    }

    public HistogramOnlyStrategy createLightweightStrategy() {
        return new HistogramOnlyStrategy(tree, pattern, extendedLightweightIntervals, estimateLightweightCosts());
    }

    public SecondaryStrategy createSecondaryStrategy(TransformedPattern.ExecutableConfiguration config) {
        return new SecondaryStrategy(tree, secondaries, pattern, lightweightCandidates, extendedLightweightIntervals, config, estimateSecondaryCosts(config));
    }

    public Estimates estimateLightweightCosts() {
        long lightweightIO = estimatePrimaryIO((long) extendedSearchSpace, extendedLightweightIntervals.size());
        return new Estimates(
                lightweightIO,
                estimatePrimaryNavigationIO(extendedLightweightIntervals.size()),
                List.of(),
                extendedLightweightIntervals.size(),
                extendedLightweightIntervals.size(),
                extendedSearchSpace / tree.getCoveredTimeInterval().getDuration());
    }

    public Estimates estimateSecondaryCosts(TransformedPattern.ExecutableConfiguration config) {

        trace("Estimating config: %s%n", config);
        var sps = config.getSubPatterns().stream()
                .map(SubPatternStats::new)
                .sorted()
                .collect(Collectors.toCollection(ArrayList::new));


        var espIter = sps.iterator();

        final SubPatternStats begin = espIter.next();
        final long initialIntervalCount = begin.matchCount;

        // Shortcut if no match is possible due to lightweight scan
        if (begin.matchCount == 0) {
            return new Estimates(0L, 0L, begin.secondaryCosts, 0, 0, 0.0);
        }

        // Secondary costs
        List<Estimates.SecondaryIndexEstimate> secondaryCosts = sps.stream()
                .flatMap(x -> x.getIndexCosts().stream())
                .collect(Collectors.toCollection(ArrayList::new));

        // Remeber if we have information about first sub-pattrern
        SubPatternStats firstSP = begin.source.getIndex() == 0 ? begin : null;

        List<Double> lambdas = new ArrayList<>();

        // More than one sub-pattern
        while (espIter.hasNext()) {
            var sp = espIter.next();
            firstSP = sp.source.getIndex() == 0 ? sp : firstSP;
            lambdas.add(1.0 / sp.interArrivalTime);
        }

        RealDistribution dist;

        if (lambdas.size() > 1) {
            dist = new HypoExponentialDistribution(lambdas);
        } else if (lambdas.size() == 1) {
            dist = new ExponentialDistribution(1.0 / lambdas.get(0));
        } else {
            dist = new FakeDist();
        }


        double candidateInWindowProp = dist.cumulativeProbability(pattern.getWindow());
        double replayLength;

        // Due to rounding issues, the prob, in rare cases drops slightly below 0
        if (candidateInWindowProp <= 0) {
            return new Estimates(0, 0, secondaryCosts, 0, 0, 0);
        }


        // Contains first and it is most selective
        if (begin.source.getIndex() == 0 && config.isExact()) {
            replayLength = estimateAverageCandidateLength(config, dist, candidateInWindowProp);
        } else if (begin.source.getIndex() == 0) {
            replayLength = pattern.getWindow();
        }
        // Contains first sub-pattern but is not most selective -- new case
        // Check, how many starts of the pattern may happen
        else if (firstSP != null) {
            double avgMatchLength = estimateAverageCandidateLength(config, dist, candidateInWindowProp);
            // Remaining towards front
            double remaining = pattern.getWindow() - avgMatchLength;
            int numInstances = (int) (remaining / firstSP.interArrivalTime);
            double durationAdd = numInstances * firstSP.interArrivalTime;

            replayLength = config.isExact() ? avgMatchLength : pattern.getWindow();
            replayLength += durationAdd;
        }
        // Contains only last sub-pattern
        else if (sps.size() == 1 && begin.source.getIndex() == pattern.getSubPatterns().size() - 1) {
            replayLength = pattern.getWindow();
        }
        // Regular behaviour
        else {
            replayLength = 2 * pattern.getWindow() - estimateAverageCandidateLength(config, dist, candidateInWindowProp);
        }

        //		double replayLength = config.getTScopeDuration(avgMatchLength);

        // Those are our values

        // New -> Consider disjoint lightweight intervals

        long unmergedIntervalCount = (long) Math.ceil(candidateInWindowProp * initialIntervalCount);
        long primaryIO = 0L;
        long coveredTimespan = 0L;
        long mergedIntervalCount = 0L;

        {
            double fullDuration = tree.getCoveredTimeInterval().getDuration();
            for (TimeInterval ti : extendedLightweightIntervals) {
                double fraction = ti.getDuration() / fullDuration;

                long localUnmergedIntervalsCount = (long) Math.ceil(unmergedIntervalCount * fraction);
                long localMergedIntervalCount = estimateMergedIntervalCount(replayLength, ti.getDuration(), localUnmergedIntervalsCount);
                long localCoveredTimespan = estimateCoveredTimespan(replayLength, ti.getDuration(), localUnmergedIntervalsCount);

                long localPrimaryIO = estimatePrimaryIO(localCoveredTimespan, localMergedIntervalCount);

                mergedIntervalCount += localMergedIntervalCount;
                coveredTimespan += localCoveredTimespan;
                primaryIO += localPrimaryIO;
            }
        }


        // TODO
        long primaryNavigationIO = estimatePrimaryNavigationIO(mergedIntervalCount);
        double temporalCoverage = (double) coveredTimespan / tree.getCoveredTimeInterval().getDuration();
        return new Estimates(primaryIO, primaryNavigationIO, secondaryCosts, mergedIntervalCount, unmergedIntervalCount, temporalCoverage);
    }

    private long estimatePrimaryNavigationIO(long intervalCount) {
        int fanOut = tree.getMaxIndexEntries();
        long pages = (long) Math.ceil((double) tree.getNumberOfEvents() / tree.getMaxLeafEntries()) / fanOut;
        long innerCount = 0L;

        while (pages > 1) {
            innerCount += Math.min(intervalCount, pages);
            pages /= fanOut;
        }

        return tree.estimateIO(innerCount);
    }

    private long estimatePrimaryIO(long coveredTimespan, long mergedIntervalCount) {
        // Estimate pages per interval
        double totalLeafPages = Math.ceil((double) tree.getNumberOfEvents() / tree.getMaxLeafEntries());
        double totalCoveredTimespan = tree.getCoveredTimeInterval().getDuration();
        double timePerPage = totalCoveredTimespan / totalLeafPages;

        long timePerMergedInterval = (coveredTimespan + mergedIntervalCount - 1) / mergedIntervalCount;
        double pagesPerInterval = timePerMergedInterval / timePerPage + 1;

        double totalPages = pagesPerInterval * mergedIntervalCount;
        return tree.estimateIO((long) Math.ceil(totalPages));


//		long leaves = (long) Math.ceil((double) tree.getNumberOfEvents() / tree.getMaxLeafEntries());
//
//		// Restrict to match area
//		leaves = (long) Math.ceil(leaves * ((double) coveredTimespan / tree.getCoveredTimeInterval().getDuration()));
//
//		return tree.estimateIO(leaves);
    }

    private double estimateAverageCandidateLength(TransformedPattern.ExecutableConfiguration config, RealDistribution dist, double candidateInWindowProp) {
        double avgMatchLength;

        if (candidateInWindowProp >= 1.0) {
            avgMatchLength = dist.getNumericalMean();
        } else {
            avgMatchLength = integrator.integrate(5000, x -> dist.density(x) * x, 0, pattern.getWindow()) / candidateInWindowProp;
        }

        return avgMatchLength + config.getMinCandidateLength();
    }

    private static class FakeDist extends AbstractRealDistribution {

        @Override
        public double density(double x) {
            return 0;
        }

        @Override
        public double cumulativeProbability(double x) {
            return 1.0;
        }

        @Override
        public double getNumericalMean() {
            return 0;
        }

        @Override
        public double getNumericalVariance() {
            return 0;
        }

        @Override
        public double getSupportLowerBound() {
            return 0;
        }

        @Override
        public double getSupportUpperBound() {
            return Double.POSITIVE_INFINITY;
        }

        @Override
        public boolean isSupportLowerBoundInclusive() {
            return true;
        }

        @Override
        public boolean isSupportUpperBoundInclusive() {
            return true;
        }

        @Override
        public boolean isSupportConnected() {
            return true;
        }
    }

    private class SubPatternStats implements Comparable<SubPatternStats> {

        final TransformedPattern.ExecutableSubPattern source;
        final long matchCount;
        final double interArrivalTime;
        final List<Estimates.SecondaryIndexEstimate> secondaryCosts;


        public SubPatternStats(TransformedPattern.ExecutableSubPattern esp) {
            this.secondaryCosts = new ArrayList<>();
            this.source = esp;
            double selectivity = 1.0;
            for (var ec : esp.getConditions()) {
                var stats = statsMap.get(ec.getId());
                selectivity *= (stats.getSelectedEventCount() / numEvents);

                long indexIO = (long) Math.ceil(Util.getIndexForRange(ec.getRange(), secondaries).estimateIO(stats.getGlobalSelectivity()));
                long indexResults = (long) Math.ceil(globalEventCount * stats.getGlobalSelectivity());

                secondaryCosts.add(new Estimates.SecondaryIndexEstimate(indexIO, indexResults));
            }
            // TODO: Uniform assumption
            this.matchCount = (long) Math.ceil(selectivity * numEvents);
            this.interArrivalTime = searchSpace / matchCount;
        }

        public List<Estimates.SecondaryIndexEstimate> getIndexCosts() {
            return secondaryCosts;
        }

        @Override
        public int compareTo(SubPatternStats o) {
            return Long.compare(matchCount, o.matchCount);
        }
    }
}
