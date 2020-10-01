package sigmod2021.pattern.cost.selection.impl;

import sigmod2021.db.core.secondaryindex.SecondaryTimeIndex;
import sigmod2021.db.core.primaryindex.impl.PrimaryIndexImpl;
import sigmod2021.pattern.cost.execution.ExecutionStrategy;
import sigmod2021.pattern.cost.execution.HistogramOnlyStrategy;
import sigmod2021.pattern.cost.selection.CostEstimator;
import sigmod2021.pattern.cost.selection.IndexSelectionStrategy;
import sigmod2021.pattern.cost.selection.PatternStats;
import sigmod2021.pattern.cost.selection.SelSubPattern;
import sigmod2021.pattern.cost.transform.SubPatternCondition;
import sigmod2021.pattern.cost.transform.TransformedPattern;
import sigmod2021.pattern.util.MultiIterator;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 *
 */
public class ParetoRecursiveStrategySelector implements IndexSelectionStrategy {

    private final TransformedPattern pattern;

    private final HistogramOnlyStrategy lw;

    private final CostEstimator estimator;


    public ParetoRecursiveStrategySelector(PrimaryIndexImpl primary, List<? extends SecondaryTimeIndex<?>> secondaries,
                                           TransformedPattern pattern, PatternStats result) {
        this.pattern = pattern;
        this.estimator = new CostEstimator(primary, secondaries, pattern, result);
        this.lw = estimator.createLightweightStrategy();
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public Result selectIndexes() {
        AtomicLong configCounter = new AtomicLong();

        long time = -System.nanoTime();
        Iterator<ExecutionStrategy> allBestResults = allBestSolutions(configCounter);

        ExecutionStrategy cmp = allBestResults.next();
        while (allBestResults.hasNext()) {
            ExecutionStrategy tmp = allBestResults.next();
            if (tmp.compareTo(cmp) < 0)
                cmp = tmp;
        }

        time += System.nanoTime();

        return new Result(cmp, configCounter.longValue(), time / 1_000_000);
    }


    private Iterator<ExecutionStrategy> allBestSolutions(AtomicLong combinationCounter) {
        List<Component> start = new ArrayList<>();
        for (var ssp : estimator.getSelectivityEnrichedSubPatterns()) {
            start.add(new Component(ssp));
        }

        Component allBestResults = processRecursive(start, combinationCounter);

        return new MultiIterator<>(allBestResults.solutions.stream().map(x -> x.iterator()).collect(Collectors.toList()));
    }

    private Component processRecursive(List<Component> elements, AtomicLong combinationCounter) {
        if (elements.size() == 1)
            return elements.get(0);

        else {
            List<Component> left = elements.subList(0, elements.size() / 2);
            List<Component> right = elements.subList(elements.size() / 2, elements.size());

            return processRecursive(left, combinationCounter).combine(processRecursive(right, combinationCounter), combinationCounter);

        }
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public String getName() {
        return "Dynamic Pareto";
    }

    private class Component {
        private int maxK;
        private List<List<ExecutionStrategy>> solutions;

        public Component(SelSubPattern ssp) {
            pattern.disableAll();
            this.maxK = ssp.getMaxSymbols();
            this.solutions = new ArrayList<>(this.maxK + 1);
            this.solutions.add(Collections.singletonList(lw));
            for (int i = 1; i <= this.maxK; i++) {
                ssp.setExecConfig(i);
                solutions.add(List.of(estimator.createSecondaryStrategy(pattern.createExecution())));
            }
        }

        private Component(int maxK, List<List<ExecutionStrategy>> solutions) {
            this.maxK = maxK;
            this.solutions = solutions;
        }


        public Component combine(Component other, AtomicLong combinationCounter) {

            int maxK = this.maxK + other.maxK;

            List<List<ExecutionStrategy>> result = new ArrayList<>();
            result.add(Collections.singletonList(lw));

            for (int k = 1; k <= maxK; k++) {

                List<ExecutionStrategy> paretoResult = new ArrayList<>();

                // TODO: May be optimized
                for (int left = k, right = 0; left >= 0; left--, right++) {
                    if (left > this.maxK || right > other.maxK)
                        continue;

                    List<ExecutionStrategy> ls = solutions.get(left);
                    List<ExecutionStrategy> rs = other.solutions.get(right);

                    for (ExecutionStrategy l : ls) {
                        rightLoop:
                        for (ExecutionStrategy r : rs) {

                            combinationCounter.incrementAndGet();

                            pattern.disableAll();
                            for (var c : l.getConfig().getConditions()) {
                                pattern.getSubPatterns().stream()
                                        .flatMap(x -> x.getConditions().stream())
                                        .filter(sc -> sc.getId().equals(c.getId()))
                                        .findFirst().ifPresent(SubPatternCondition::enable);
                            }
                            for (var c : r.getConfig().getConditions()) {
                                pattern.getSubPatterns().stream()
                                        .flatMap(x -> x.getConditions().stream())
                                        .filter(sc -> sc.getId().equals(c.getId()))
                                        .findFirst().ifPresent(SubPatternCondition::enable);
                            }

                            ExecutionStrategy s = estimator.createSecondaryStrategy(pattern.createExecution());

                            ListIterator<ExecutionStrategy> lIter = paretoResult.listIterator();

                            while (lIter.hasNext()) {
                                ExecutionStrategy check = lIter.next();
                                // Current solution is not part of pareto front
                                if (check.dominates(s))
                                    continue rightLoop;
                                    // Current solution dominates another one
                                else if (s.dominates(check))
                                    lIter.remove();
                            }
                            paretoResult.add(s);
                        }
                    }
                }
                result.add(paretoResult);
            }
            return new Component(maxK, result);
        }
    }
}
