package sigmod2021.pattern.experiments.util;

import sigmod2021.common.IncompatibleTypeException;
import sigmod2021.db.core.secondaryindex.EventID;
import sigmod2021.db.core.secondaryindex.SecondaryTimeIndex;
import sigmod2021.db.core.primaryindex.queries.range.AttributeRange;
import sigmod2021.pattern.cost.selection.PatternStats;
import sigmod2021.pattern.cost.transform.SubPattern;
import sigmod2021.pattern.cost.transform.SubPatternCondition;
import sigmod2021.pattern.cost.transform.TransformedPattern;
import sigmod2021.pattern.replay.ReplayPatternMatcher;
import sigmod2021.pattern.util.IntervalMergeCursor;
import sigmod2021.db.util.TimeInterval;
import sigmod2021.esp.ql.TranslatorException;
import sigmod2021.event.Event;
import xxl.core.cursors.Cursor;
import xxl.core.util.Pair;

import java.util.*;
import java.util.stream.Collectors;

public class BudgetStrategy {

    private final AccessCountingPrimaryIndex tree;
    private final List<? extends AccessCountingLSMTimeIndex<?>> secondaries;
    private final TransformedPattern pattern;
    private final List<RankedElement> rankedElements;
    private final List<TransformedPattern.ExecutableCondition> rankedConditions = new ArrayList<>();
    public BudgetStrategy(AccessCountingPrimaryIndex tree, List<? extends AccessCountingLSMTimeIndex<?>> secondaries, TransformedPattern pattern, PatternStats stats) {
        this.tree = tree;
        this.secondaries = secondaries;
        this.pattern = pattern;
        this.rankedElements = new ArrayList<>();

        for (SubPattern sp : pattern.getSubPatterns()) {
            double[] spSel = {1};
            stats.getConditionStats().stream()
                    .filter(x -> x.getConditionId().subPatternIndex == sp.getIndex())
                    .forEach(x -> spSel[0] *= x.getGlobalSelectivity());

            stats.getConditionStats().stream()
                    .filter(x -> x.getConditionId().subPatternIndex == sp.getIndex())
                    .forEach(x -> rankedElements.add(new RankedElement(x.getConditionId(), spSel[0], x.getGlobalSelectivity())));
        }

        Collections.sort(rankedElements);

        pattern.enableAll();

        for (RankedElement re : rankedElements)
            rankedConditions.add(pattern.createExecution().getConditions().stream().filter(x -> x.getId().equals(re.id)).findFirst().orElseThrow());
    }

    private Cursor<TimeInterval> performIndexScan() {
        List<TimeInterval> currentIntervals = List.of(tree.getCoveredTimeInterval());

        long indexCosts = 0L;
        long prunedData = 0L;
        long oldexpectedPrimaryIO = estimateBytesToRead(currentIntervals);


        /// First Index
        var i = rankedConditions.iterator();
        if (i.hasNext()) {
            currentIntervals = queryIndex(i.next()).stream()
                    .map(x -> new TimeInterval(x.getTimestamp() - pattern.getWindow(), x.getTimestamp() + pattern.getWindow()))
                    .collect(Collectors.toCollection(LinkedList::new));

            long newPrimaryIO = estimateBytesToRead(currentIntervals);

            prunedData = Math.max(0L, oldexpectedPrimaryIO - newPrimaryIO);
            indexCosts = getSecondaryIndexCosts();

            oldexpectedPrimaryIO = newPrimaryIO;
        }

        // Add indexes

        while (i.hasNext() && indexCosts <= prunedData && oldexpectedPrimaryIO > 0) {
            resetSecondaryIndexCosts();

            var indexResuts = queryIndex(i.next());

            merge(currentIntervals, indexResuts);

            long newPrimaryIO = estimateBytesToRead(currentIntervals);

            prunedData = Math.max(0L, oldexpectedPrimaryIO - newPrimaryIO);
            indexCosts = getSecondaryIndexCosts();

            oldexpectedPrimaryIO = newPrimaryIO;
        }

        return new IntervalMergeCursor<>(currentIntervals.iterator());
    }

    private void merge(List<TimeInterval> intervals, List<EventID> indexResults) {
        if (indexResults.isEmpty()) {
            intervals.clear();
            return;
        }

        var ii = indexResults.iterator();
        var current = ii.next();


        for (var li = intervals.listIterator(); li.hasNext(); ) {
            var chk = li.next();

            while (current != null && current.getTimestamp() < chk.getT1())
                current = ii.hasNext() ? ii.next() : null;

            if (current == null || current.getTimestamp() > chk.getT2())
                li.remove();
        }
    }

    private long estimateBytesToRead(List<TimeInterval> intervals) {
        long[] duration = {0L};
        var mc = new IntervalMergeCursor<>(intervals.iterator());
        mc.open();
        mc.forEachRemaining(x -> duration[0] += x.getDuration());
        mc.close();

        double totalleafPages = Math.ceil(1.0 * tree.getNumberOfEvents() / tree.getMaxLeafEntries());
        double ratio = 1.0 * duration[0] / tree.getCoveredTimeInterval().getDuration();
        return tree.estimateIO((long) Math.ceil(totalleafPages * ratio));
    }

    public Cursor<Event> execute() throws IncompatibleTypeException, TranslatorException {
        Cursor<TimeInterval> regions = performIndexScan();
        final ReplayPatternMatcher rpm = new ReplayPatternMatcher(tree, pattern.getDefinition());
        return rpm.executeMultiRegions(regions);

    }

    private void resetSecondaryIndexCosts() {
        secondaries.stream().forEach(AccessCountingLSMTimeIndex::resetMeasurement);
    }

    private long getSecondaryIndexCosts() {
        return secondaries.stream().mapToLong(AccessCountingLSMTimeIndex::getTotalReadBytes).sum();
    }

    private List<EventID> queryIndex(TransformedPattern.ExecutableCondition ec) {
        AttributeRange range = ec.getRange();

        SecondaryTimeIndex idx = secondaries.stream()
                .filter(s -> s.getAttributeName().equalsIgnoreCase(range.getName()))
                .findFirst()
                .orElseThrow();

        Cursor<Pair<?, EventID>> unsorted = idx.rangeQueryID(range.getLower(), range.getUpper());
        unsorted.open();
        List<EventID> result = new ArrayList<>();
        unsorted.forEachRemaining(x -> result.add(x.getElement2()));
        unsorted.close();
        result.sort(Comparator.comparingLong(EventID::getTimestamp));
        return result;
    }

    private static class RankedElement implements Comparable<RankedElement> {
        final SubPatternCondition.ConditionId id;
        final double subPatternSelectivity;
        final double conditionSelectivity;

        public RankedElement(SubPatternCondition.ConditionId id, double subPatternSelectivity, double conditionSelectivity) {
            this.id = id;
            this.subPatternSelectivity = subPatternSelectivity;
            this.conditionSelectivity = conditionSelectivity;
        }

        @Override
        public int compareTo(RankedElement o) {
            return Double.compare(conditionSelectivity, o.conditionSelectivity);
//            int result =  Double.compare(subPatternSelectivity,o.subPatternSelectivity);
//            return result == 0 ? Double.compare(conditionSelectivity,o.conditionSelectivity) : result;
        }
    }

}
