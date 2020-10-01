package sigmod2021.pattern.cost.selection.impl;

import sigmod2021.db.core.secondaryindex.SecondaryTimeIndex;
import sigmod2021.db.core.primaryindex.impl.PrimaryIndexImpl;
import sigmod2021.pattern.cost.execution.ExecutionStrategy;
import sigmod2021.pattern.cost.selection.CostEstimator;
import sigmod2021.pattern.cost.selection.IndexSelectionStrategy;
import sigmod2021.pattern.cost.selection.PatternStats;
import sigmod2021.pattern.cost.selection.SelSubPattern;
import sigmod2021.pattern.cost.transform.TransformedPattern;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

/**
 *
 */
public class GreedyStrategySelector implements IndexSelectionStrategy {

    private final TransformedPattern pattern;

    private final PatternStats lwResult;

    private final CostEstimator estimator;

    public GreedyStrategySelector(PrimaryIndexImpl primary, List<? extends SecondaryTimeIndex<?>> secondaries,
                                  TransformedPattern pattern, PatternStats result) {
        this.pattern = pattern;
        this.lwResult = result;
        this.estimator = new CostEstimator(primary, secondaries, pattern, result);
    }


    /**
     * @{inheritDoc}
     */
    @Override
    public Result selectIndexes() {

        List<SelSubPattern.ICondition> conditions = estimator.getSelectivityEnrichedSubPatterns().stream().flatMap(x -> x.getConditions().stream()).collect(Collectors.toList());

        Collections.sort(conditions);

        ExecutionStrategy cmp = estimator.createLightweightStrategy();

        long configCounter = 0L;

        long time = -System.nanoTime();
        if (!lwResult.getMatchIntervals().isEmpty()) {
            pattern.disableAll();
            for (Iterator<SelSubPattern.ICondition> ci = conditions.iterator(); ci.hasNext(); ) {
                configCounter++;
                ci.next().getCondition().enable();
                ExecutionStrategy tmp = estimator.createSecondaryStrategy(pattern.createExecution());
                if (tmp.compareTo(cmp) < 0)
                    cmp = tmp;
            }
        }
        time += System.nanoTime();
        return new Result(cmp, configCounter, time / 1_000_000);
    }


    /**
     * @{inheritDoc}
     */
    @Override
    public String getName() {
        return "Greedy";
    }
}
