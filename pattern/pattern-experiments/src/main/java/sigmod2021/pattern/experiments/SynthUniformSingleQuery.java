package sigmod2021.pattern.experiments;

import sigmod2021.common.IncompatibleTypeException;
import sigmod2021.db.DBException;
import sigmod2021.pattern.cost.execution.ExecutionStrategy;
import sigmod2021.pattern.cost.selection.impl.NaiveStrategySelector;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class SynthUniformSingleQuery {

    static final Random rand = new Random(4711);

    private static void process(PatternMatcher def, AccessCountingPrimaryIndex primary,
                                List<? extends AccessCountingLSMTimeIndex<?>> secondaries)
            throws TranslatorException, IncompatibleTypeException {
        System.out.println("Processing pattern: " + def.getPattern() + ", window: " + def.getWithin());

        TransformedPattern pattern = Util.transformPattern(def, primary.getSchema());

        System.out.println("Computing pattern stats");
        var estimationResult = ExperimentsBasics.computePatternStats(primary, secondaries, pattern);

        NaiveStrategySelector suggest = new NaiveStrategySelector(primary, secondaries, pattern, estimationResult);

        List<ExecutionStrategy> strategies = suggest.getAllStrategies()
                .stream()
                .sorted()
                .collect(Collectors.toCollection(ArrayList::new));


        ExperimentsBasics.IOMeasures iom = new ExperimentsBasics.IOMeasures();
        List<Long> execTimes = new ArrayList<>();

        // Warm up
        System.out.println("Warming Up");
        for (var s : strategies.subList(0, Math.min(10, strategies.size()))) {
            ExperimentsBasics.consume(s.execute());
        }

        System.out.println("Executing based on estimates... (" + strategies.size() + " strategies)");
        {
            int counter = 1;
            for (ExecutionStrategy es : strategies) {
                System.out.print((counter++) + ", ");
                execTimes.add(executeStrategy(primary, secondaries, es, iom));
            }
            System.out.println();
        }

        System.out.println(iom);
        System.out.println("Execution Times: " + execTimes);
    }

    private static long executeStrategy(AccessCountingPrimaryIndex primary,
                                        List<? extends AccessCountingLSMTimeIndex<?>> secondaries,
                                        ExecutionStrategy es, ExperimentsBasics.IOMeasures iom) throws IncompatibleTypeException, TranslatorException {
        iom.resetMeasures(primary, secondaries);
        long time = -System.currentTimeMillis();
        ExperimentsBasics.consume(es.execute());
        time += System.currentTimeMillis();
        iom.updateAndReset(es, primary, secondaries);
        return time;
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

            System.out.println("Index height is  : " + primary.getHeight());
            System.out.println("Compression ratio: " + primary.getCompressionRatio());

            // Warm Up os' page-cache
            System.out.print("Warming up page cache...");
            System.out.println(" Done! (" + ExperimentsBasics.warmUpPageCache(primary) + ")");
            PatternMatcher def = ExperimentsBasics.randomPattern(rand, 0.1, 300, 8);
            process(def, primary, secondaries);
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
