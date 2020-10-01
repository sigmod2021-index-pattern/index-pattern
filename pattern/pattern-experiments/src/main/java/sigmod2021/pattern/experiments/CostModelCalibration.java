package sigmod2021.pattern.experiments;

import sigmod2021.common.IncompatibleTypeException;
import sigmod2021.db.DBException;
import sigmod2021.db.core.secondaryindex.EventID;
import sigmod2021.pattern.experiments.util.AccessCountingLSMTimeIndex;
import sigmod2021.pattern.experiments.util.ExperimentsBasics;
import sigmod2021.pattern.replay.ReplayPatternMatcher;
import sigmod2021.pattern.experiments.util.ExperimentUtil;
import sigmod2021.pattern.experiments.util.ExperimentUtil.ExperimentConfig;
import sigmod2021.pattern.experiments.data.DataSource;
import sigmod2021.pattern.experiments.data.MultiDataGenerator;
import sigmod2021.pattern.experiments.util.AccessCountingPrimaryIndex;
import sigmod2021.esp.api.epa.PatternMatcher;
import sigmod2021.esp.api.epa.Stream;
import sigmod2021.esp.api.epa.pattern.Output;
import sigmod2021.esp.api.epa.pattern.regex.Atom;
import sigmod2021.esp.api.epa.pattern.regex.Sequence;
import sigmod2021.esp.api.epa.pattern.symbol.Binding;
import sigmod2021.esp.api.epa.pattern.symbol.Bindings;
import sigmod2021.esp.api.epa.pattern.symbol.Symbol;
import sigmod2021.esp.api.epa.pattern.symbol.Symbols;
import sigmod2021.esp.api.expression.arithmetic.atomic.DoubleConstant;
import sigmod2021.esp.api.expression.arithmetic.atomic.Variable;
import sigmod2021.esp.api.expression.predicate.Greater;
import sigmod2021.esp.api.expression.predicate.Less;
import sigmod2021.esp.ql.TranslatorException;
import xxl.core.util.Pair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;

/**
 *
 */
public class CostModelCalibration {

    private static final int RUNS = 10;

    private static CMeasure measurePrimary(AccessCountingPrimaryIndex p) {

        List<CMeasure> measures = new ArrayList<>();

        for (int i = 0; i < RUNS; i++) {
            p.resetMeasurement();
            p.emptyBuffers();

            long time = -System.nanoTime();
            var c = p.query();
            c.open();
            while (c.hasNext()) {
                c.next();
            }
            c.close();
            time += System.nanoTime();

            measures.add(new CMeasure(p.getTotalReadBytes(), time));
        }

        measures.sort(null);
        return measures.get(measures.size() / 2);
    }

    private static CMeasure measureEval(AccessCountingPrimaryIndex p) throws IncompatibleTypeException, TranslatorException {

        List<CMeasure> measures = new ArrayList<>();

        PatternMatcher def = new PatternMatcher(
                new Stream("TEST"),
                30_000,
                new Symbols(
                        new Symbol('A', new Less(new Variable("A4"), new DoubleConstant(Double.MIN_VALUE)), new Bindings(new Binding("OUT", new Variable("A4")))),
                        new Symbol('B', new Greater(new Variable("A4"), new DoubleConstant(Double.MIN_VALUE)), new Bindings())
                ),
                new Sequence(new Atom('A'), new Atom('B')),
                new Output("OUT")
        );


        for (int i = 0; i < RUNS; i++) {
            long counter = 0L;
            p.resetMeasurement();
            p.emptyBuffers();

            long time = -System.nanoTime();
            ReplayPatternMatcher rpm = new ReplayPatternMatcher(p, def);
            var results = rpm.executeDirect(p.query());
            results.open();
            while (results.hasNext()) {
                results.next();
            }
            results.close();
            time += System.nanoTime();
            measures.add(new CMeasure(p.getTotalReadBytes(), time));
        }

        measures.sort(null);
        return measures.get(measures.size() / 2);
    }

    private static CMeasure measureSecondary(AccessCountingLSMTimeIndex<Double> s) {

        List<CMeasure> measures = new ArrayList<>();
        for (int i = 0; i < RUNS; i++) {
            s.resetMeasurement();
            s.emptyBuffers();

            long time = -System.nanoTime();
            var c = s.rangeQueryID(0.0, 1.0);
            c.open();
            while (c.hasNext()) {
                c.next();
            }
            c.close();
            time += System.nanoTime();
            measures.add(new CMeasure(s.getTotalReadBytes(), time));
        }

        measures.sort(null);
        return measures.get(measures.size() / 2);
    }

    private static CMeasure measureSort(AccessCountingLSMTimeIndex<Double> s, double selectivity) {

        List<CMeasure> measures = new ArrayList<>();
        for (int i = 0; i < RUNS; i++) {
            s.resetMeasurement();
            s.emptyBuffers();
            List<EventID> events = new ArrayList<>();

            var c = s.rangeQueryID(0.3, 0.3 + selectivity);
            c.open();
            while (c.hasNext()) {
                c.forEachRemaining(p -> events.add(p.getElement2()));
            }
            c.close();

            final Random rand = new Random(1337L);
            long time = -System.nanoTime();
            Collections.shuffle(events, rand);
            Collections.sort(events);
            time += System.nanoTime();

            measures.add(new CMeasure(events.size(), time));
        }
        measures.sort(null);
        return measures.get(measures.size() / 2);
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
            var sIdx = secondaries.stream().filter(idx -> idx.getAttributeName().equals("A4")).findFirst().orElseThrow();

            // Warm up page cache
            System.out.println("Warming up page cahe.");
            ExperimentsBasics.warmUpPageCache(primary);
            var c = sIdx.rangeQueryID(0.0, 1.0);
            c.open();
            while (c.hasNext()) {
                c.next();
            }
            c.close();


            System.out.println("Measuring Primary Index Layout");
            CMeasure pm = measurePrimary(primary);

            System.out.println("Measuring Secondary Index Layout");
            CMeasure sm = measureSecondary(sIdx);

            System.out.println("Measuring Sorting of Secondary Index Results");
            CMeasure sortm = measureSort(sIdx, 0.1);

            System.out.println("Measuring Pattern execution");
            CMeasure em = measureEval(primary);

            double pv = (double) pm.nanos / pm.items;
            double sv = (double) sm.nanos / sm.items;
            double sortv = (double) sortm.nanos / (sortm.items * (Math.log(sortm.items) / Math.log(2.0)));
            double ev = (double) em.nanos / em.items;


            System.out.println("Weight PIO : " + (ev / pv));
            System.out.println("Weight SIO : " + (sv / pv));
            System.out.println("Weight SORT: " + (sortv / pv));

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

    static class CMeasure implements Comparable<CMeasure> {
        private final long items;
        private final long nanos;

        public CMeasure(long items, long nanos) {
            this.items = items;
            this.nanos = nanos;
        }

        @Override
        public int compareTo(CMeasure o) {
            return Long.compare(nanos, o.nanos);
        }
    }

}
