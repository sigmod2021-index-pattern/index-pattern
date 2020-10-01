package sigmod2021.pattern.experiments.util;

import sigmod2021.db.DBException;
import sigmod2021.db.core.IOMeasuring;
import sigmod2021.db.core.secondaryindex.SecondaryTimeIndex;
import sigmod2021.db.core.primaryindex.impl.PrimaryIndexImpl;
import sigmod2021.db.core.primaryindex.impl.legacy.ImmutableParams;
import sigmod2021.db.core.primaryindex.queries.range.AttributeRange;
import sigmod2021.pattern.cost.execution.Estimates;
import sigmod2021.pattern.cost.execution.ExecutionStrategy;
import sigmod2021.pattern.cost.selection.PatternStats;
import sigmod2021.pattern.cost.transform.SubPatternCondition;
import sigmod2021.pattern.cost.transform.TransformedPattern;
import sigmod2021.db.event.TID;
import sigmod2021.pattern.experiments.util.ExperimentUtil.ExperimentConfig;
import sigmod2021.pattern.experiments.data.DataSource;
import sigmod2021.db.util.TimeInterval;
import sigmod2021.db.util.Util;
import sigmod2021.esp.api.epa.PatternMatcher;
import sigmod2021.esp.api.epa.Stream;
import sigmod2021.esp.api.epa.pattern.Output;
import sigmod2021.esp.api.epa.pattern.regex.Atom;
import sigmod2021.esp.api.epa.pattern.regex.KleeneStar;
import sigmod2021.esp.api.epa.pattern.regex.Pattern;
import sigmod2021.esp.api.epa.pattern.regex.Sequence;
import sigmod2021.esp.api.epa.pattern.symbol.Binding;
import sigmod2021.esp.api.epa.pattern.symbol.Bindings;
import sigmod2021.esp.api.epa.pattern.symbol.Symbol;
import sigmod2021.esp.api.epa.pattern.symbol.Symbols;
import sigmod2021.esp.api.expression.arithmetic.atomic.DoubleConstant;
import sigmod2021.esp.api.expression.arithmetic.atomic.Variable;
import sigmod2021.esp.api.expression.logical.True;
import sigmod2021.esp.api.expression.predicate.Between;
import sigmod2021.event.Attribute;
import sigmod2021.event.Event;
import sigmod2021.event.EventSchema;
import sigmod2021.event.TimeRepresentation;
import xxl.core.collections.containers.Container;
import xxl.core.cursor.LimitingIterator;
import xxl.core.cursors.Cursor;
import xxl.core.util.Pair;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 *
 */
public class ExperimentsBasics {

    public static final int ESTIMATION_LEVEL = 3;
    public static final int NUM_EVENTS = 50_000_000;

    public static List<ExecutionStrategy> cloneWithGroundTruth(List<ExecutionStrategy> strategies, IOMeasures iom) {
        List<ExecutionStrategy> truth = new ArrayList<>();
        for (int i = 0; i < strategies.size(); i++) {
            var orig = strategies.get(i);
            truth.add(orig.cloneWithNewEstimates(iom.real.get(i)));
        }
        truth.sort(null);
        return truth;
    }

    public static final long consume(Cursor<?> c) {
        long results = 0L;
        c.open();
        while (c.hasNext()) {
            results++;
            c.next();
        }
        return results;
    }

    public static long warmUpPageCache(PrimaryIndexImpl primary) {
        Container con = primary.getTree().container();
        long pageCount = 0L;
        for (var i = con.getAll(con.ids(), true); i.hasNext(); ) {
            i.next();
            pageCount++;
        }
        return pageCount;
    }

    ;

    public static final PatternStats computePatternStats(AccessCountingPrimaryIndex primary,
                                                         List<? extends AccessCountingLSMTimeIndex<?>> secondaries, TransformedPattern tp) {
        List<SubPatternCondition<?>> conditions = tp.collectConditions();

        final long totalCount = primary.getNumberOfEvents();
        final TimeInterval interval = primary.getCoveredTimeInterval();

        List<PatternStats.ConditionStats> stats = new ArrayList<>();

        for (int i = 0; i < conditions.size(); i++) {
            AttributeRange<?> r = conditions.get(i).getRange();

            double[] selectivity = {1.0};

            secondaries.stream().filter(x -> x.getAttributeName().equalsIgnoreCase(r.getName())).findFirst()
                    .ifPresent(idx -> {
                        Comparable lower = r.getLower();
                        Comparable upper = r.getUpper();
                        var cursor = ((AccessCountingLSMTimeIndex) idx).rangeQueryID(lower, upper);
                        cursor.open();
                        double count = 0.0;
                        while (cursor.hasNext()) {
                            cursor.next();
                            count += 1;
                        }
                        selectivity[0] = count / totalCount;
                    });

            final SubPatternCondition.ConditionId id = conditions.get(i).getId();

            stats.add(new PatternStats.ConditionStats() {

                @Override
                public Iterator<TimeInterval> intervals() {
                    return List.of(interval).iterator();
                }

                @Override
                public SubPatternCondition.ConditionId getConditionId() {
                    return id;
                }

                @Override
                public long getTotalEventCount() {
                    return totalCount;
                }

                @Override
                public long getTotalDuration() {
                    return interval.getDuration();
                }

                @Override
                public double getSelectedEventCount() {
                    return totalCount * selectivity[0];
                }

                @Override
                public int getNumIntervals() {
                    return 1;
                }

                @Override
                public double getLocalSelectivity() {
                    return selectivity[0];
                }

                @Override
                public double getGlobalSelectivity() {
                    return selectivity[0];
                }
            });
        }

//		for ( var cs : stats ) {
//			System.out.println("Condition selectivity: " + cs.getGlobalSelectivity());
//		}

        return new PatternStats() {

            @Override
            public List<MatchInterval> getMatchIntervals() {
                return List.of(new MatchInterval() {

                    @Override
                    public TimeInterval getInterval() {
                        return interval;
                    }

                    @Override
                    public long getEventCount() {
                        return totalCount;
                    }

                    @Override
                    public long getExtendEnd() {
                        return interval.getT1();
                    }

                    @Override
                    public long getExtendBegin() {
                        return interval.getT2();
                    }

                });
            }

            @Override
            public List<ConditionStats> getConditionStats() {
                return stats;
            }

        };
    }

    public static PatternStats toUniformAssumption(AccessCountingPrimaryIndex primary, PatternStats hResult) {
        long totalEvents = primary.getNumberOfEvents();
        TimeInterval fullInterval = primary.getCoveredTimeInterval();

        List<PatternStats.ConditionStats> css = new ArrayList<>();
        for (PatternStats.ConditionStats cs : hResult.getConditionStats()) {
            css.add(new PatternStats.ConditionStats() {

                @Override
                public SubPatternCondition.ConditionId getConditionId() {
                    return cs.getConditionId();
                }

                @Override
                public long getTotalEventCount() {
                    return totalEvents;
                }

                @Override
                public long getTotalDuration() {
                    return fullInterval.getDuration();
                }

                @Override
                public int getNumIntervals() {
                    return 1;
                }

                @Override
                public double getSelectedEventCount() {
                    return totalEvents * cs.getGlobalSelectivity();
                }

                @Override
                public Iterator<TimeInterval> intervals() {
                    return List.of(fullInterval).iterator();
                }

                @Override
                public double getGlobalSelectivity() {
                    return cs.getGlobalSelectivity();
                }

                @Override
                public double getLocalSelectivity() {
                    return cs.getGlobalSelectivity();
                }
            });
        }

        return new PatternStats() {

            @Override
            public List<MatchInterval> getMatchIntervals() {
                return List.of(new MatchInterval() {
                    @Override
                    public TimeInterval getInterval() {
                        return fullInterval;
                    }

                    @Override
                    public long getExtendEnd() {
                        return fullInterval.getT2();
                    }

                    @Override
                    public long getExtendBegin() {
                        return fullInterval.getT1();
                    }

                    @Override
                    public long getEventCount() {
                        return totalEvents;
                    }
                });
            }

            @Override
            public List<ConditionStats> getConditionStats() {
                return css;
            }
        };
    }

    public static PatternMatcher randomPattern(Random rand, double maxSelectivity, long window, final int numSymbols) {
        return randomPattern(rand, maxSelectivity, window, numSymbols, "A4");
    }

    // Generates a random pattern with numSymbols symbols
    public static PatternMatcher randomPattern(Random rand, double maxSelectivity, long window, final int numSymbols,
                                               String firstAttrib) {

        final String varOut = "OUT";

        int remaining = numSymbols;
        char[] names = new char[51];
        for (char c = 'A'; c <= 'Z'; c++) {
            names[c - 'A'] = c;
        }
        for (char c = 'a'; c < 'z'; c++) {
            names[c - 'a' + 26] = c;
        }

        int next = 0;
        char skip = 'z';

        List<Symbol> symbols = new ArrayList<>();
        List<Pattern> pattern = new ArrayList<>();

        symbols.add(new Symbol(skip, new True()));

        while (remaining > 0) {
            int take = rand.nextInt(remaining) + 1;
            remaining -= take;

            for (int i = 0; i < take; i++) {
                String attr = (next == 0) ? firstAttrib : "A4";

                Bindings bs = (next == 0) ? new Bindings(new Binding(varOut, new Variable(attr))) : new Bindings();

                char name = names[next++];
                double selectivity = rand.nextDouble() * maxSelectivity;
                double begin = 1.0;
                while (begin + selectivity > 1.0)
                    begin = rand.nextDouble();

                symbols.add(new Symbol(name,
                        new Between(new Variable(attr), new DoubleConstant(begin), new DoubleConstant(begin + selectivity)),
                        bs));
                pattern.add(new Atom(name));
            }
            if (remaining > 0)
                pattern.add(new KleeneStar(new Atom(skip)));
        }

        return new PatternMatcher(new Stream("dummy"), window,
                new Symbols(symbols),
                new Sequence(pattern),
                new Output(varOut));
    }


    public static Pair<AccessCountingPrimaryIndex, List<AccessCountingLSMTimeIndex<Double>>> getTree(
            ExperimentConfig cfg,
            DataSource source)
            throws DBException, IOException, InterruptedException, ExecutionException {
        Path myPath = cfg.basePath.resolve("db").resolve("synth");

        AccessCountingPrimaryIndex tree = null;

        List<AccessCountingLSMTimeIndex<Double>> idxs = new ArrayList<>();

        EventSchema schema = source.getSchema();
        for (String attrib : List.of("A0", "A1", "A2", "A3", "A4")) {
            schema.byName(attrib).setProperty("index", "true").setProperty("secondaryIndex", "true");
        }

        if (Files.exists(myPath)) {
            tree = new AccessCountingPrimaryIndex(myPath, cfg.mParams);

            if (tree.getNumberOfEvents() != NUM_EVENTS || !tree.getSchema().equals(schema)) {
                System.err.println("Existing tree is not in sync with experimental setup. Deleting.");
                tree.close();
                Util.deleteDirectoryRecursively(myPath);
            } else {
                for (Attribute a : tree.getSchema()) {
                    if (a.getProperty("secondaryIndex") != null && a.getProperty("secondaryIndex").equalsIgnoreCase("true")) {
                        idxs.add(new AccessCountingLSMTimeIndex<>(myPath, schema, a.getName(), tree.getTree().BLOCK_SIZE,
                                cfg.mParams.isUseDirectIO()));
                    }
                }
            }

        }
        if (tree == null) {
            System.err.println("Creating new Tree with " + NUM_EVENTS + " events");
            cfg.iParams.setContainerType(ImmutableParams.ContainerType.BLOCK);
            tree = new AccessCountingPrimaryIndex(myPath, schema, TimeRepresentation.POINT, cfg.iParams, cfg.mParams);
            for (Attribute a : tree.getSchema()) {
                if (a.getProperty("secondaryIndex") != null && a.getProperty("secondaryIndex").equalsIgnoreCase("true")) {
                    idxs.add(new AccessCountingLSMTimeIndex<>(myPath, schema, a.getName(), tree.getTree().BLOCK_SIZE,
                            cfg.mParams.isUseDirectIO()));
                }
            }

            long count = 0L;
            Iterator<Event> data = new LimitingIterator<>(NUM_EVENTS, source.iterator());
            while (data.hasNext()) {
                Event e = data.next();
                final TID res = tree.insert(e).get();
                final long seq = tree.getNumberOfEvents() - 1;
                for (SecondaryTimeIndex<Double> idx : idxs)
                    idx.insertEvent(e, res, seq);

                if (count > 0 && count % 1_000_000L == 0)
                    System.out.println("Inserted " + count + " events.");

            }
        }
        return new Pair<>(tree, idxs);
    }

    public static class IOMeasures {
        private List<Estimates> estimated = new ArrayList<>();
        private List<Estimates> real = new ArrayList<>();
        private List<Long> realSIO = new ArrayList<>();

        public List<Estimates> getEstimated() {
            return estimated;
        }

        public List<Estimates> getReal() {
            return real;
        }

        public List<Long> getRealSIO() {
            return realSIO;
        }

        public void updateAndReset(ExecutionStrategy s, AccessCountingPrimaryIndex primary,
                                   List<? extends AccessCountingLSMTimeIndex<?>> secondaries) {
            estimated.add(s.getEstimates());

            Estimates actual = new Estimates(
                    primary.getReadLeafBytes(),
                    primary.getReadInnerBytes(),
                    // TODO: FIXME
                    s.getEstimates().getSecondary(),
//				List.of(new Estimates.SecondaryIndexEstimate(sio,0L)),
                    primary.getQueriedIntervals(),
                    -1L,
                    0.0
            );

            real.add(actual);

            realSIO.add(secondaries.stream().collect(Collectors.summarizingLong(AccessCountingLSMTimeIndex::getTotalReadBytes)).getSum());


            resetMeasures(primary, secondaries);
        }

        public void duplicateLast() {
            estimated.add(estimated.get(estimated.size() - 1));
            real.add(real.get(real.size() - 1));
        }

        public void resetMeasures(AccessCountingPrimaryIndex primary,
                                  List<? extends AccessCountingLSMTimeIndex<?>> secondaries) {
            primary.emptyBuffers();
            primary.resetMeasurement();

            for (var si : secondaries) {
                si.resetMeasurement();
                si.emptyBuffers();
            }
        }

        public String toString() {
            var pioErrors = calcErrors(x -> x.getPrimaryLeafIO() + x.getPrimaryNavigationIO());
            var pioLeafErrors = calcErrors(Estimates::getPrimaryLeafIO);
            var pioInnerErrors = calcErrors(Estimates::getPrimaryNavigationIO);
            var sioErrors = calcSIOErrors();

            double errAccumPIO = pioErrors.stream().collect(Collectors.averagingDouble(x -> x));
            double errAccumPIOLeaf = pioLeafErrors.stream().collect(Collectors.averagingDouble(x -> x));
            double errAccumPIOInner = pioInnerErrors.stream().collect(Collectors.averagingDouble(x -> x));
            double errAccumSIO = sioErrors.stream().collect(Collectors.averagingDouble(x -> x));
            StringBuilder result = new StringBuilder();
            result.append("IOMeasurements [").append(String.format("%n"));
            result.append("  Expected PIO          : ").append(estimated.stream().map(x -> x.getPrimaryLeafIO() + x.getPrimaryNavigationIO()).map(IOMeasuring::formatMiB).collect(Collectors.toList())).append(String.format("%n"));
            result.append("  Actual PIO            : ").append(real.stream().map(x -> x.getPrimaryLeafIO() + x.getPrimaryNavigationIO()).map(IOMeasuring::formatMiB).collect(Collectors.toList())).append(String.format("%n"));
            result.append("  Expected PIO (Leaves) : ").append(estimated.stream().map(Estimates::getPrimaryLeafIO).map(IOMeasuring::formatMiB).collect(Collectors.toList())).append(String.format("%n"));
            result.append("  Actual PIO (leaves)   : ").append(real.stream().map(Estimates::getPrimaryLeafIO).map(IOMeasuring::formatMiB).collect(Collectors.toList())).append(String.format("%n"));
            result.append("  Expected PIO (inner)  : ").append(estimated.stream().map(Estimates::getPrimaryNavigationIO).map(IOMeasuring::formatMiB).collect(Collectors.toList())).append(String.format("%n"));
            result.append("  Actual PIO (inner)    : ").append(real.stream().map(Estimates::getPrimaryNavigationIO).map(IOMeasuring::formatMiB).collect(Collectors.toList())).append(String.format("%n"));
            result.append("  Expected SIO          : ").append(estimated.stream().map(Estimates::getSecondaryIO).map(IOMeasuring::formatMiB).collect(Collectors.toList())).append(String.format("%n"));
            result.append("  Actual SIO            : ").append(realSIO.stream().map(IOMeasuring::formatMiB).collect(Collectors.toList())).append(String.format("%n"));
            result.append("  Expected Intervals    : ").append(estimated.stream().map(Estimates::getReplayIntervals).collect(Collectors.toList())).append(String.format("%n"));
            result.append("  Actual Intervals      : ").append(real.stream().map(Estimates::getReplayIntervals).collect(Collectors.toList())).append(String.format("%n"));
            result.append("  Expected Raw Intervals: ").append(estimated.stream().map(Estimates::getUnmergedIntervals).collect(Collectors.toList())).append(String.format("%n"));
            result.append("  Actual Raw Intervals  : ").append(real.stream().map(Estimates::getUnmergedIntervals).collect(Collectors.toList())).append(String.format("%n"));
            result.append("  Expected Coverage     : ").append(estimated.stream().map(Estimates::getTemporalCoverage).collect(Collectors.toList())).append(String.format("%n"));
            result.append("  Actual Coverage       : ").append(real.stream().map(Estimates::getTemporalCoverage).collect(Collectors.toList())).append(String.format("%n"));
            result.append("  Errors PIO            : ").append(pioErrors).append(String.format("%n"));
            result.append("  Errors PIO (leaves)   : ").append(pioLeafErrors).append(String.format("%n"));
            result.append("  Errors PIO (inner)    : ").append(pioInnerErrors).append(String.format("%n"));
            result.append("  Errors SIO            : ").append(sioErrors).append(String.format("%n"));
            result.append("  Error PIO             : ").append(errAccumPIO).append(String.format("%n"));
            result.append("  Error PIO (leaves)    : ").append(errAccumPIOLeaf).append(String.format("%n"));
            result.append("  Error PIO (inner)     : ").append(errAccumPIOInner).append(String.format("%n"));
            result.append("  Error SIO             : ").append(errAccumSIO).append(String.format("%n"));
            result.append("  Expected Score        : ").append(estimated.stream().map(Estimates::getScore).collect(Collectors.toList())).append(String.format("%n"));
            result.append("  Actual Score          : ").append(real.stream().map(Estimates::getScore).collect(Collectors.toList())).append(String.format("%n"));
            result.append("]");
            return result.toString();
        }

        private List<Double> calcSIOErrors() {
            if (estimated.size() != realSIO.size())
                throw new RuntimeException("Unequal list lengths");

            List<Double> result = new ArrayList<>(estimated.size());
            var li = estimated.iterator();
            var ri = realSIO.iterator();
            while (li.hasNext()) {
                Long l = li.next().getSecondaryIO();
                Long r = ri.next();

                long delta = Math.abs(r - l);
                if (delta == 0)
                    result.add(0.0);
                else if (r > 0)
                    result.add((double) delta / r);
                // Don't add this
//				else
//					result.add(1.0);
            }
            return result;

        }

        private List<Double> calcErrors(Function<Estimates, Long> extract) {
            if (estimated.size() != real.size())
                throw new RuntimeException("Unequal list lengths");

            List<Double> result = new ArrayList<>(estimated.size());
            var li = estimated.iterator();
            var ri = real.iterator();
            while (li.hasNext()) {
                Long l = extract.apply(li.next());
                Long r = extract.apply(ri.next());

                long delta = Math.abs(r - l);
                if (delta == 0)
                    result.add(0.0);
                else if (r > 0)
                    result.add((double) delta / r);
                // Don't add this
//				else
//					result.add(1.0);
            }
            return result;
        }
    }
}
