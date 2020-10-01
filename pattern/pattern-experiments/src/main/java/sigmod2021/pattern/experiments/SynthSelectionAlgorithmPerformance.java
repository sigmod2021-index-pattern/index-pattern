package sigmod2021.pattern.experiments;

import sigmod2021.common.IncompatibleTypeException;
import sigmod2021.db.DBException;
import sigmod2021.pattern.cost.selection.IndexSelectionStrategy;
import sigmod2021.pattern.cost.selection.IndexSelectionStrategyFactory;
import sigmod2021.pattern.cost.selection.PatternStats;
import sigmod2021.pattern.cost.selection.impl.AllStrategySelector;
import sigmod2021.pattern.cost.selection.impl.GreedyStrategySelector;
import sigmod2021.pattern.cost.selection.impl.ParetoRecursiveStrategySelector;
import sigmod2021.pattern.cost.transform.TransformedPattern;
import sigmod2021.pattern.experiments.util.AccessCountingLSMTimeIndex;
import sigmod2021.pattern.experiments.util.ExperimentsBasics;
import sigmod2021.pattern.util.Util;
import sigmod2021.pattern.experiments.util.ExperimentUtil;
import sigmod2021.pattern.experiments.util.ExperimentUtil.ExperimentConfig;
import sigmod2021.pattern.experiments.data.DataSource;
import sigmod2021.pattern.experiments.data.MultiDataGenerator;
import sigmod2021.pattern.experiments.util.AccessCountingPrimaryIndex;
import sigmod2021.esp.api.epa.PatternMatcher;
import sigmod2021.esp.ql.TranslatorException;
import xxl.core.util.Pair;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

/**
 *
 */
public class SynthSelectionAlgorithmPerformance {

    static final long SEED = 4711;

    static final Random RAND = new Random(SEED);

    static final double MAX_SELECTIVITY = 0.1;

    static final long WINDOW = 300;

    static final int WARM_UP_RUNS = 100;

    static final int RUNS = 1000;

    private static Result execute(int runs, int numSymbols, AccessCountingPrimaryIndex primary, List<? extends AccessCountingLSMTimeIndex<?>> secondaries) throws TranslatorException, IncompatibleTypeException {
        List<IndexSelectionStrategyFactory> strategyFactories = Arrays.asList(
                AllStrategySelector::new,
                GreedyStrategySelector::new,
                ParetoRecursiveStrategySelector::new
        );

        Result r = new Result();

        for (int i = 0; i < runs; i++) {
            PatternMatcher def = ExperimentsBasics.randomPattern(RAND, MAX_SELECTIVITY, WINDOW, numSymbols);
            TransformedPattern pattern = Util.transformPattern(def, primary.getSchema());

            PatternStats stats = ExperimentsBasics.computePatternStats(primary,secondaries,pattern);

            for (var sf : strategyFactories) {
                long time = -System.nanoTime();
                IndexSelectionStrategy strategy = sf.create(primary, secondaries, pattern, stats);
                var sr = strategy.selectIndexes();
                time += System.nanoTime();
                r.add(strategy.getName(), time);
                r.addInvocation(strategy.getName(), sr.getCostModelInvocations());
            }
        }
        return r;
    }

    /**
     * @param args
     */
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

            System.out.print("Warming Up...");
            for (int i = 2; i <= 32; i *= 2) {
                execute(WARM_UP_RUNS, i, primary, secondaries);
            }
            System.out.println(" Done!");

            System.out.println("Runnning Experiment");
            for (int i = 2; i <= 32; i *= 2) {
                System.out.println("  Processing " + i + " symbols.");
                var r = execute(RUNS, i, primary, secondaries);
                System.out.println(r);
                System.out.println("========================================================");
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

    static class Result {
        Map<String, List<Long>> timings = new HashMap<>();
        Map<String, List<Long>> cmInvocations = new HashMap<>();

        public void add(String id, long timeMS) {
            List<Long> results = timings.get(id);
            if (results == null) {
                results = new ArrayList<>();
                timings.put(id, results);
            }
            results.add(timeMS);
        }

        public void addInvocation(String id, long count) {
            List<Long> results = cmInvocations.get(id);
            if (results == null) {
                results = new ArrayList<>();
                cmInvocations.put(id, results);
            }
            results.add(count);
        }

        /**
         * @{inheritDoc}
         */
        @Override
        public String toString() {
            StringBuilder result = new StringBuilder();
            result.append("Results [").append(String.format("%n"));
            result.append("  Timings: [").append(String.format("%n"));
            for (var e : timings.entrySet()) {
                var avg = e.getValue().stream().collect(Collectors.summarizingLong(x -> x));
                result.append("    ").append(e.getKey()).append(": ").append(e.getValue()).append(String.format("%n"));
                result.append("    ").append(e.getKey()).append(" average: ").append(avg).append(String.format("%n"));
            }
            result.append("  ]");


            result.append("  Combinations tried: [").append(String.format("%n"));
            for (var e : cmInvocations.entrySet()) {
                var avg = e.getValue().stream().collect(Collectors.summarizingLong(x -> x));
                result.append("    ").append(e.getKey()).append(": ").append(e.getValue()).append(String.format("%n"));
                result.append("    ").append(e.getKey()).append(" average: ").append(avg).append(String.format("%n"));
            }
            result.append("  ]");

            result.append("]");

            return result.toString();
        }
    }

}
