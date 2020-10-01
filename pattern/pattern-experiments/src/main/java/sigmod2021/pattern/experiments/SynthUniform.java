package sigmod2021.pattern.experiments;

import sigmod2021.common.IncompatibleTypeException;
import sigmod2021.db.DBException;
import sigmod2021.pattern.cost.execution.ExecutionStrategy;
import sigmod2021.pattern.cost.selection.HistogramScanner;
import sigmod2021.pattern.cost.selection.IndexSelectionStrategy;
import sigmod2021.pattern.cost.selection.IndexSelectionStrategyFactory;
import sigmod2021.pattern.cost.selection.PatternStats;
import sigmod2021.pattern.cost.selection.impl.AllStrategySelector;
import sigmod2021.pattern.cost.selection.impl.GreedyStrategySelector;
import sigmod2021.pattern.cost.selection.impl.HistogramOnlyStrategySelector;
import sigmod2021.pattern.cost.selection.impl.ParetoRecursiveStrategySelector;
import sigmod2021.pattern.cost.transform.TransformedPattern;
import sigmod2021.pattern.replay.ReplayPatternMatcher;
import sigmod2021.pattern.util.Util;
import sigmod2021.pattern.experiments.util.ExperimentUtil;
import sigmod2021.pattern.experiments.data.DataSource;
import sigmod2021.pattern.experiments.data.MultiDataGenerator;
import sigmod2021.pattern.experiments.util.AccessCountingLSMTimeIndex;
import sigmod2021.pattern.experiments.util.AccessCountingPrimaryIndex;
import sigmod2021.pattern.experiments.util.ExperimentsBasics;
import sigmod2021.pattern.experiments.util.BudgetStrategy;
import sigmod2021.esp.api.epa.PatternMatcher;
import sigmod2021.esp.ql.TranslatorException;
import xxl.core.util.Pair;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 *
 */
public class SynthUniform {

    static final long SEED = 4711;
    static final Random RAND = new Random(SEED);
    static final double MAX_SELECTIVITY = 0.1;
    static final long WINDOW = 300;
    static final int WARM_UP_SYMBOLS = 8;
    static final int WARM_UP_RUNS = 10;
    static final int RUNS = 100;

    private static void processSelection(PatternMatcher def, AccessCountingPrimaryIndex primary,
                                         List<? extends AccessCountingLSMTimeIndex<?>> secondaries) {

        TransformedPattern pattern = Util.transformPattern(def, primary.getSchema());
        var lwScanner = new HistogramScanner(pattern);
        var lwResult = lwScanner.estimate(primary, ExperimentsBasics.ESTIMATION_LEVEL);

        List<IndexSelectionStrategyFactory> strategyFactories = Arrays.asList(
                AllStrategySelector::new,
                GreedyStrategySelector::new,
                ParetoRecursiveStrategySelector::new
        );

        Map<String, IndexSelectionStrategy.Result> results = new HashMap<>(strategyFactories.size());

        for (var sf : strategyFactories) {
            IndexSelectionStrategy strategy = sf.create(primary, secondaries, pattern, lwResult);
            results.put(strategy.getName(), strategy.selectIndexes());
        }
    }


    private static void warmUp(AccessCountingPrimaryIndex primary, List<? extends AccessCountingLSMTimeIndex<?>> secondaries) throws TranslatorException, IncompatibleTypeException {
        System.out.println("Performing WarmUp...");

        System.out.print("  Processing selection...");
        for (int i = 0; i < WARM_UP_RUNS; i++) {
            processSelection(ExperimentsBasics.randomPattern(RAND, MAX_SELECTIVITY, WINDOW, WARM_UP_SYMBOLS), primary, secondaries);
        }
        System.out.println(" Done!");

        PatternMatcher def = ExperimentsBasics.randomPattern(RAND, MAX_SELECTIVITY, WINDOW, WARM_UP_SYMBOLS);
        TransformedPattern pattern = Util.transformPattern(def, primary.getSchema());
        var lwScanner = new HistogramScanner(pattern);
        var lwResult = lwScanner.estimate(primary, ExperimentsBasics.ESTIMATION_LEVEL);

        PatternStats stats = ExperimentsBasics.computePatternStats(primary, secondaries, pattern);

        System.out.print("  Processing TODS...");
        BudgetStrategy ts = new BudgetStrategy(primary, secondaries, pattern, stats);
        ExperimentsBasics.consume(ts.execute());
        System.out.println(" Done!");

        System.out.print("  Processing Replay...");
        ReplayPatternMatcher rpm = new ReplayPatternMatcher(primary, def);
        ExperimentsBasics.consume(rpm.execute());
        System.out.println(" Done!");

        {
            System.out.print("  Processing Lightweight...");
            ExecutionStrategy lws = new HistogramOnlyStrategySelector(primary, secondaries, pattern, lwResult).selectIndexes().getStrategy();
            ExperimentsBasics.consume(lws.execute());
            System.out.println(" Done!");
        }

        {
            System.out.print("  Processing Secondary...");
            ExecutionStrategy all = new ParetoRecursiveStrategySelector(primary, secondaries, pattern, lwResult).selectIndexes().getStrategy();
            ExperimentsBasics.consume(all.execute());
            System.out.println(" Done!");
        }
    }

    private static void randomPatternsExperiment(AccessCountingPrimaryIndex primary, List<? extends AccessCountingLSMTimeIndex<?>> secondaries,
                                                 int numSymbols, int numRuns) throws TranslatorException, IncompatibleTypeException {

        System.out.println("Processing Experiment (" + numSymbols + " symbols, " + numRuns + " runs):");

        IndexSelectionStrategyFactory allFactory = AllStrategySelector::new;
        IndexSelectionStrategyFactory greedyFactory = GreedyStrategySelector::new;
        IndexSelectionStrategyFactory optimalFactory = ParetoRecursiveStrategySelector::new;

        Map<String, List<Long>> processingTimes = new HashMap<>();
        ExperimentsBasics.IOMeasures measures = new ExperimentsBasics.IOMeasures();


        System.out.print("Processing Run: ");

        for (int i = 0; i < numRuns; i++) {
            PatternMatcher def = ExperimentsBasics.randomPattern(RAND, MAX_SELECTIVITY, WINDOW, numSymbols);

            TransformedPattern pattern = Util.transformPattern(def, primary.getSchema());
            PatternStats stats = ExperimentsBasics.computePatternStats(primary, secondaries, pattern);


			{
				long time = -System.nanoTime();
				ReplayPatternMatcher rpm = new ReplayPatternMatcher(primary, def);
				ExperimentsBasics.consume(rpm.execute());
				time += System.nanoTime();

				List<Long> times = processingTimes.getOrDefault("Replay", new ArrayList<>());
				times.add(time / 1_000_000);
				processingTimes.put("Replay", times);
			}

            {
                long time = -System.nanoTime();
                BudgetStrategy ts = new BudgetStrategy(primary, secondaries, pattern, stats);
                ExperimentsBasics.consume(ts.execute());
                time += System.nanoTime();

                List<Long> times = processingTimes.getOrDefault("TODS", new ArrayList<>());
                times.add(time / 1_000_000);
                processingTimes.put("TODS", times);
            }

			var all     = allFactory.create(primary,secondaries,pattern,stats).selectIndexes().getStrategy();
			var greedy  = greedyFactory.create(primary,secondaries,pattern,stats).selectIndexes().getStrategy();
            var optimal = optimalFactory.create(primary, secondaries, pattern, stats).selectIndexes().getStrategy();

            measures.resetMeasures(primary, secondaries);
            long time = -System.nanoTime();
			ExperimentsBasics.consume(all.execute());
			time += System.nanoTime();
			measures.updateAndReset(all, primary, secondaries);

			{
				List<Long> times = processingTimes.getOrDefault("All", new ArrayList<>());
				times.add(time / 1_000_000);
				processingTimes.put("All", times);
			}

			// Greedy
			if ( !greedy.isEquivalent(all) ) {
				measures.resetMeasures(primary, secondaries);
				time = -System.nanoTime();
				ExperimentsBasics.consume(greedy.execute());
				time += System.nanoTime();
				measures.updateAndReset(greedy, primary, secondaries);
			}
			else {
				measures.duplicateLast();
			}

			{
				List<Long> times = processingTimes.getOrDefault("Greedy", new ArrayList<>());
				times.add(time / 1_000_000);
				processingTimes.put("Greedy", times);
			}

			// Optimal
			if ( !optimal.isEquivalent(greedy) ) {
                measures.resetMeasures(primary, secondaries);
                time = -System.nanoTime();
                ExperimentsBasics.consume(optimal.execute());
                time += System.nanoTime();
                measures.updateAndReset(optimal, primary, secondaries);
			}
			else {
				measures.duplicateLast();
			}

            {
                List<Long> times = processingTimes.getOrDefault("Optimal", new ArrayList<>());
                times.add(time / 1_000_000);
                processingTimes.put("Optimal", times);
            }
        }
        System.out.println();
        for (var e : processingTimes.entrySet()) {
            System.out.printf("  %25s: %s%n", e.getKey(), e.getValue());
        }
        System.out.println(measures);
    }


    public static void main(String[] args) throws DBException, IOException, InterruptedException,
            ExecutionException, TranslatorException, IncompatibleTypeException {
        ExperimentUtil.ExperimentConfig cfg = ExperimentUtil.getConfig();

        System.out.println("Base path is: " + cfg.basePath.toString());

        DataSource ds = new MultiDataGenerator(50_000_000);

        Pair<AccessCountingPrimaryIndex, List<AccessCountingLSMTimeIndex<Double>>> t = null;
        try {
            t = ExperimentsBasics.getTree(cfg, ds);

            var primary = t.getElement1();
            var secondaries = t.getElement2();

            System.out.println("Index height is  : " + primary.getHeight());
            System.out.println("Compression ratio: " + primary.getCompressionRatio());

            warmUp(primary, secondaries);

            // Warm Up os' page-cache
            System.out.print("Warming up page cache...");
            System.out.println(" Done! (" + ExperimentsBasics.warmUpPageCache(primary) + ")");

            for (int i = 2; i <= 32; i *= 2) {
                randomPatternsExperiment(primary, secondaries, i, RUNS);
            }
        } finally {
            if (t != null) {
                try {
                    t.getElement1().close();

                } catch (Exception e) {
                    e.printStackTrace();
                }
                for (var si : t.getElement2())
                    si.close();
            }
        }

    }

}
