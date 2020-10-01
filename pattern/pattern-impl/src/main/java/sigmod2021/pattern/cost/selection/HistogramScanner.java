package sigmod2021.pattern.cost.selection;

import sigmod2021.db.core.primaryindex.impl.PrimaryIndexImpl;
import sigmod2021.pattern.cost.estimation.ConditionInfo;
import sigmod2021.pattern.cost.estimation.ConditionRunner;
import sigmod2021.pattern.cost.estimation.SubTreeDescription;
import sigmod2021.pattern.cost.estimation.results.ConditionCandidates;
import sigmod2021.pattern.cost.estimation.results.MatchCandidate;
import sigmod2021.pattern.cost.estimation.results.WindowedPatternRunner;
import sigmod2021.pattern.cost.transform.SubPatternCondition;
import sigmod2021.pattern.cost.transform.TransformedPattern;
import sigmod2021.db.util.TimeInterval;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

/**
 *
 */
public class HistogramScanner {

    private final TransformedPattern pattern;

    /**
     * Creates a new PatternEstimator instance
     */
    public HistogramScanner(TransformedPattern pattern) {
        this.pattern = pattern;
    }


    public PatternStats estimate(PrimaryIndexImpl tree, int level) {

        long time = -System.currentTimeMillis();

        pattern.enableAll();

        var config = pattern.createExecution();

        // Collect statistics for each sub-pattern
        List<ConditionRunner> result = new ArrayList<>();
        for (var c : config.getConditions()) {
            result.add(new ConditionRunner(tree.getSchema(), c));
        }

        // Windowed analysis
        final WindowedPatternRunner windowed = new WindowedPatternRunner(result.size(), pattern.getWindow());

        long[] counter = {0L};
        tree.walkLevel(level, lwa -> {
            counter[0]++;
            // Collect stats for each sub-pattern
            List<ConditionInfo> cis = new ArrayList<>();
            for (var runner : result) {
                cis.add(runner.update(lwa));
            }
            // Update window
            windowed.update(new SubTreeDescription(lwa.getIndex(), lwa.getCoveredTimeInterval(), lwa.getEventCount(), cis));
        });


        for (var runner : result) {
            runner.close();
        }

        var matches = windowed.getResults();
        var stats = new ArrayList<PatternStats.ConditionStats>();

        var spri = result.iterator();

        for (int i = 0; i < config.getConditions().size(); i++) {
            final int idx = i;
            List<ConditionCandidates> spcs = matches.stream().map(x -> x.getInfos().get(idx)).collect(Collectors.toList());
            stats.add(new ConditionStatsImpl(spri.next(), spcs));
        }

        time += System.currentTimeMillis();

        System.out.println("Visited " + counter[0] + " index entries in " + time + " ms.");

        return new HistogramScanResult(matches, stats);
    }

    private static class HistogramScanResult implements PatternStats {

        private final List<MatchInterval> candidates;
        private final List<ConditionStats> stats;

        /**
         * Creates a new Result instance
         * @param candidates
         * @param stats
         */
        HistogramScanResult(List<MatchCandidate> candidates, List<ConditionStats> stats) {
            this.candidates = new ArrayList<>(candidates);
            this.stats = stats;
        }

        /**
         * @return the candidates
         */
        public List<MatchInterval> getMatchIntervals() {
            return this.candidates;
        }

        /**
         * @return the stats
         */
        public List<ConditionStats> getConditionStats() {
            return this.stats;
        }

        /**
         * @{inheritDoc}
         */
        @Override
        public String toString() {
            return String.format("EstimationResult [%n  candidates: %s%n  stats: %s%n]", this.candidates, this.stats);
        }
    }


    private static class ConditionStatsImpl implements PatternStats.ConditionStats {
        final List<TimeInterval> intervals = new ArrayList<>();
        final SubPatternCondition.ConditionId id;
        final double globalSelectivity;
        final double selectedEventCount;
        final long totalEventCount;
        final long totalDuration;

        private ConditionStatsImpl(ConditionRunner cr, List<ConditionCandidates> candidates) {
            this.id = cr.getCondition().getId();
            long totalDuration = 0L;
            long totalEventCount = 0;
            double selectedEventCount = 0.0;

            for (var ccs : candidates) {
                for (var cc : ccs) {
                    intervals.add(cc.getInterval());
                    totalDuration += cc.getInterval().getDuration();
                    totalEventCount += cc.getEventCount();
                    selectedEventCount += (cc.getEventCount() * cc.getLocalSelectivity());
                }
            }

            this.totalDuration = totalDuration;
            this.totalEventCount = totalEventCount;
            this.selectedEventCount = selectedEventCount;
            this.globalSelectivity = cr.getGlobalSelectivity();
        }

        @Override
        public SubPatternCondition.ConditionId getConditionId() {
            return id;
        }

        /**
         * @return the totalEventCount
         */
        public long getTotalEventCount() {
            return this.totalEventCount;
        }

        /**
         * @return the totalDuration
         */
        public long getTotalDuration() {
            return this.totalDuration;
        }

        /**
         * @return the numIntervals
         */
        public int getNumIntervals() {
            return this.intervals.size();
        }

        public Iterator<TimeInterval> intervals() {
            return intervals.iterator();
        }

        public double getGlobalSelectivity() {
            return globalSelectivity;
        }

        public double getLocalSelectivity() {
            return selectedEventCount / totalEventCount;
        }

        @Override
        public double getSelectedEventCount() {
            return selectedEventCount;
        }

        /**
         * @{inheritDoc}
         */
        @Override
        public String toString() {
            StringBuilder result = new StringBuilder();
            result.append("ConditionStats [").append(String.format("%n"));
            result.append("  id                : ").append(id).append(String.format("%n"));
            result.append("  totalDuration     : ").append(String.format("%d", totalDuration)).append(String.format("%n"));
            result.append("  totalEventCount   : ").append(String.format("%d", totalEventCount)).append(String.format("%n"));
            result.append("  selectedEventCount: ").append(String.format("%d", (long) selectedEventCount)).append(String.format("%n"));
            result.append("  localSelectivity  : ").append(String.format("%.4f", getLocalSelectivity())).append(String.format("%n"));
            result.append("]");
            return result.toString();
        }
    }


}
