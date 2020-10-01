package sigmod2021.pattern.cost.selection.impl;

import sigmod2021.db.core.secondaryindex.SecondaryTimeIndex;
import sigmod2021.db.core.primaryindex.impl.PrimaryIndexImpl;
import sigmod2021.pattern.cost.execution.ExecutionStrategy;
import sigmod2021.pattern.cost.selection.IndexSelectionStrategy;
import sigmod2021.pattern.cost.selection.PatternStats;
import sigmod2021.pattern.cost.selection.CostEstimator;
import sigmod2021.pattern.cost.transform.TransformedPattern;

import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class NaiveStrategySelector implements IndexSelectionStrategy {

    private final TransformedPattern pattern;

    private final PatternStats lwResult;

    private final CostEstimator estimator;


    public NaiveStrategySelector(PrimaryIndexImpl primary, List<? extends SecondaryTimeIndex<?>> secondaries,
                                 TransformedPattern pattern, PatternStats result) {
        this.pattern = pattern;
        this.lwResult = result;
        this.estimator = new CostEstimator(primary, secondaries, pattern, result);
    }


    public List<ExecutionStrategy> getAllStrategies() {
        List<ExecutionStrategy> result = new ArrayList<>();
        result.add(estimator.createLightweightStrategy());
        if (!lwResult.getMatchIntervals().isEmpty()) {
            var configs = pattern.iterateConfigurations();
            while (configs.hasNext()) {
                result.add(estimator.createSecondaryStrategy(configs.next()));
            }
        }
        return result;
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public Result selectIndexes() {
        long configCounter = 0L;

        ExecutionStrategy cmp = estimator.createLightweightStrategy();

        long time = -System.nanoTime();
        if (!lwResult.getMatchIntervals().isEmpty()) {
            var configs = pattern.iterateConfigurations();
            while (configs.hasNext()) {
                configCounter++;
                ExecutionStrategy tmp = estimator.createSecondaryStrategy(configs.next());
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
        return "Naive";
    }
}
