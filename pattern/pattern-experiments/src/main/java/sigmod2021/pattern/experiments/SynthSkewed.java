package sigmod2021.pattern.experiments;

import sigmod2021.common.IncompatibleTypeException;
import sigmod2021.db.DBException;
import sigmod2021.pattern.cost.execution.ExecutionStrategy;
import sigmod2021.pattern.cost.selection.HistogramScanner;
import sigmod2021.pattern.cost.selection.IndexSelectionStrategy;
import sigmod2021.pattern.cost.selection.IndexSelectionStrategyFactory;
import sigmod2021.pattern.cost.selection.impl.AllStrategySelector;
import sigmod2021.pattern.cost.selection.impl.GreedyStrategySelector;
import sigmod2021.pattern.cost.selection.impl.HistogramOnlyStrategySelector;
import sigmod2021.pattern.cost.selection.impl.ParetoRecursiveStrategySelector;
import sigmod2021.pattern.cost.transform.TransformedPattern;
import sigmod2021.pattern.experiments.util.AccessCountingLSMTimeIndex;
import sigmod2021.pattern.experiments.util.AccessCountingPrimaryIndex;
import sigmod2021.pattern.experiments.util.BudgetStrategy;
import sigmod2021.pattern.experiments.util.ExperimentsBasics;
import sigmod2021.pattern.replay.ReplayPatternMatcher;
import sigmod2021.pattern.util.Util;
import sigmod2021.pattern.experiments.util.ExperimentUtil;
import sigmod2021.pattern.experiments.util.ExperimentUtil.ExperimentConfig;
import sigmod2021.pattern.experiments.data.DataSource;
import sigmod2021.pattern.experiments.data.MultiDataGenerator;
import sigmod2021.esp.api.epa.PatternMatcher;
import sigmod2021.esp.ql.TranslatorException;
import xxl.core.util.Pair;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 *
 */
public class SynthSkewed {

    static final long SEED = 4711;
    static final Random RAND = new Random(SEED);
    static final int HISTOGRAM_LEVEL = 3;
    static final double MAX_SELECTIVITY = 0.1;
    static final long WINDOW = 300;
    static final int WARM_UP_SYMBOLS = 8;
    static final int WARM_UP_RUNS = 10;
    static final int RUNS = 100;


    private static void processSelection(PatternMatcher def, AccessCountingPrimaryIndex primary,
                                         List<? extends AccessCountingLSMTimeIndex<?>> secondaries) {

        TransformedPattern pattern = Util.transformPattern(def, primary.getSchema());
        var hScanner = new HistogramScanner(pattern);
        var hResult = hScanner.estimate(primary, HISTOGRAM_LEVEL);

        List<IndexSelectionStrategyFactory> strategyFactories = Arrays.asList(
                AllStrategySelector::new,
                GreedyStrategySelector::new,
                ParetoRecursiveStrategySelector::new
        );

        Map<String, IndexSelectionStrategy.Result> results = new HashMap<>(strategyFactories.size());

        for (var sf : strategyFactories) {
            IndexSelectionStrategy strategy = sf.create(primary, secondaries, pattern, hResult);
            results.put(strategy.getName(), strategy.selectIndexes());
        }
    }


    private static void warmUp(AccessCountingPrimaryIndex primary, List<? extends AccessCountingLSMTimeIndex<?>> secondaries) throws TranslatorException, IncompatibleTypeException {
        System.out.println("Performing WarmUp...");

        System.out.print("  Processing selection...");
        for (int i = 0; i < WARM_UP_RUNS; i++) {
            processSelection(ExperimentsBasics.randomPattern(RAND, MAX_SELECTIVITY, WINDOW, WARM_UP_SYMBOLS, "A0"), primary, secondaries);
        }
        System.out.println(" Done!");

        PatternMatcher def = ExperimentsBasics.randomPattern(RAND, MAX_SELECTIVITY, WINDOW, WARM_UP_SYMBOLS, "A0");
        TransformedPattern pattern = Util.transformPattern(def, primary.getSchema());
        var hScanner = new HistogramScanner(pattern);
        var hResult = hScanner.estimate(primary, HISTOGRAM_LEVEL);

		System.out.print("  Processing Replay...");
		ReplayPatternMatcher rpm = new ReplayPatternMatcher(primary, def);
		ExperimentsBasics.consume(rpm.execute());
		System.out.println(" Done!");

        System.out.print("  Processing TODS...");
        BudgetStrategy ts = new BudgetStrategy(primary, secondaries, pattern, hResult);
        ExperimentsBasics.consume(ts.execute());
        System.out.println(" Done!");

		System.out.print("  Processing Histogram only...");
		ExecutionStrategy lws = new HistogramOnlyStrategySelector(primary, secondaries, pattern, hResult).selectIndexes().getStrategy();
		ExperimentsBasics.consume(lws.execute());
		System.out.println(" Done!");

		System.out.print("  Processing Secondary...");
		ExecutionStrategy all = new AllStrategySelector(primary, secondaries, pattern, hResult).selectIndexes().getStrategy();
		ExperimentsBasics.consume(all.execute());
		System.out.println(" Done!");
    }

    private static void randomPatternsExperiment(AccessCountingPrimaryIndex primary, List<? extends AccessCountingLSMTimeIndex<?>> secondaries,
                                                 int numSymbols, int numRuns, String firstAttr) throws TranslatorException, IncompatibleTypeException {

        System.out.println("Processing Experiment (" + numSymbols + " symbols, " + numRuns + " runs):");

        List<IndexSelectionStrategyFactory> strategyFactories = Arrays.asList(
                GreedyStrategySelector::new,
                ParetoRecursiveStrategySelector::new
        );

        Map<String, List<Long>> processingTimes = new HashMap<>();
        ExperimentsBasics.IOMeasures measures = new ExperimentsBasics.IOMeasures();


        System.out.print("Processing Run: ");

        for (int i = 0; i < numRuns; i++) {
            PatternMatcher def = ExperimentsBasics.randomPattern(RAND, MAX_SELECTIVITY, WINDOW, numSymbols, firstAttr);

            System.out.print((i + 1) + ", ");

            TransformedPattern pattern = Util.transformPattern(def, primary.getSchema());
            var hScanner = new HistogramScanner(pattern);
            var hStats = hScanner.estimate(primary, HISTOGRAM_LEVEL);
            var uStats = ExperimentsBasics.toUniformAssumption(primary, hStats);

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
				IndexSelectionStrategy all = new AllStrategySelector(primary,secondaries,pattern,uStats);
				ExecutionStrategy es = all.selectIndexes().getStrategy();
				measures.resetMeasures(primary, secondaries);

				long time = -System.nanoTime();
				ExperimentsBasics.consume(es.execute());
				time += System.nanoTime();

				measures.updateAndReset(es, primary, secondaries);

				List<Long> times = processingTimes.getOrDefault(all.getName(), new ArrayList<>());
				times.add(time/1_000_000);
				processingTimes.put(all.getName(), times);
			}

            {
                BudgetStrategy ts = new BudgetStrategy(primary, secondaries, pattern, uStats);
                measures.resetMeasures(primary, secondaries);
                long time = -System.nanoTime();
                ExperimentsBasics.consume(ts.execute());
                time += System.nanoTime();
                measures.resetMeasures(primary, secondaries);

                List<Long> times = processingTimes.getOrDefault("TODS", new ArrayList<>());
                times.add(time / 1_000_000);
                processingTimes.put("TODS", times);
            }


			for ( var sf : strategyFactories ) {
				for ( boolean useStats : List.of(true,false) ) {
					IndexSelectionStrategy strategy = sf.create(primary, secondaries, pattern, useStats ? hStats : uStats);
					ExecutionStrategy es = strategy.selectIndexes().getStrategy();

					measures.resetMeasures(primary, secondaries);

					long time = -System.nanoTime();
					ExperimentsBasics.consume(es.execute());
					time += System.nanoTime();

					measures.updateAndReset(es, primary, secondaries);

					final String name = useStats ? strategy.getName() + "*" : strategy.getName();
					List<Long> times = processingTimes.getOrDefault(name, new ArrayList<>());
					times.add(time / 1_000_000);
					processingTimes.put(name, times);
				}
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
        ExperimentConfig cfg = ExperimentUtil.getConfig();

        System.out.println("Base path is: " + cfg.basePath.toString());

        DataSource ds = new MultiDataGenerator(50_000_000);

        Pair<AccessCountingPrimaryIndex, List<AccessCountingLSMTimeIndex<Double>>> t = null;
        try {
            t = ExperimentsBasics.getTree(cfg, ds);

            var primary = t.getElement1();
            var secondaries = t.getElement2();

            warmUp(primary, secondaries);

            // Warm Up os' page-cache
            System.out.print("Warming up page cache...");
            System.out.println(" Done! (" + ExperimentsBasics.warmUpPageCache(primary) + ")");

            for (String attr : Arrays.asList("A0", "A1", "A2", "A3", "A4")) {
                System.out.println("======================================");
                System.out.println("Processing attribute: " + attr);
                randomPatternsExperiment(primary, secondaries, 8, RUNS, attr);
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
