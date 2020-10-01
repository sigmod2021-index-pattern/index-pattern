package sigmod2021.pattern.experiments;

import sigmod2021.common.IncompatibleTypeException;
import sigmod2021.db.DBException;
import sigmod2021.db.core.primaryindex.impl.PrimaryWithSecondaryIndexImpl;
import sigmod2021.pattern.cost.selection.PatternStats;
import sigmod2021.pattern.cost.selection.HistogramScanner;
import sigmod2021.pattern.cost.selection.impl.NaiveStrategySelector;
import sigmod2021.pattern.cost.transform.SubPatternCondition;
import sigmod2021.pattern.cost.transform.TransformedPattern;
import sigmod2021.pattern.experiments.util.AccessCountingLSMTimeIndex;
import sigmod2021.pattern.experiments.util.AccessCountingPrimaryIndex;
import sigmod2021.pattern.experiments.util.ExperimentUtil;
import sigmod2021.pattern.experiments.util.ExperimentsBasics;
import sigmod2021.pattern.util.Util;
import sigmod2021.db.util.TimeInterval;
import sigmod2021.esp.api.epa.PatternMatcher;
import sigmod2021.esp.api.epa.Stream;
import sigmod2021.esp.api.epa.pattern.Output;
import sigmod2021.esp.api.epa.pattern.regex.Atom;
import sigmod2021.esp.api.epa.pattern.regex.KleenePlus;
import sigmod2021.esp.api.epa.pattern.regex.Pattern;
import sigmod2021.esp.api.epa.pattern.regex.Sequence;
import sigmod2021.esp.api.epa.pattern.symbol.Binding;
import sigmod2021.esp.api.epa.pattern.symbol.Bindings;
import sigmod2021.esp.api.epa.pattern.symbol.Symbol;
import sigmod2021.esp.api.epa.pattern.symbol.Symbols;
import sigmod2021.esp.api.expression.arithmetic.atomic.DoubleConstant;
import sigmod2021.esp.api.expression.arithmetic.atomic.PrevVariable;
import sigmod2021.esp.api.expression.arithmetic.atomic.Variable;
import sigmod2021.esp.api.expression.arithmetic.compund.numeric.Multiply;
import sigmod2021.esp.api.expression.logical.And;
import sigmod2021.esp.api.expression.predicate.GreaterEq;
import sigmod2021.esp.api.expression.predicate.Less;
import sigmod2021.esp.api.expression.predicate.LessEq;
import sigmod2021.esp.ql.TranslatorException;
import sigmod2021.event.Attribute;
import sigmod2021.event.EventSchema;
import sigmod2021.event.TimeRepresentation;
import sigmod2021.event.impl.SimpleEvent;
import xxl.core.util.Pair;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 *
 */
public class LandingPattern {

    private static EventSchema getSchema() {

        EventSchema schema = new EventSchema(
                new Attribute("TIME", Attribute.DataType.LONG),
                new Attribute("ICAO24", Attribute.DataType.STRING),
                new Attribute("LAT", Attribute.DataType.DOUBLE),
                new Attribute("LON", Attribute.DataType.DOUBLE),
                new Attribute("VELOCITY", Attribute.DataType.DOUBLE),
                new Attribute("HEADING", Attribute.DataType.DOUBLE),
                new Attribute("VERTRATE", Attribute.DataType.DOUBLE),
                new Attribute("CALLSIGN", Attribute.DataType.STRING),
                new Attribute("ONGROUND", Attribute.DataType.BYTE),
                new Attribute("ALERT", Attribute.DataType.BYTE),
                new Attribute("SPI", Attribute.DataType.BYTE),
                new Attribute("SQUAWK", Attribute.DataType.INTEGER),
                new Attribute("BAROALTITUDE", Attribute.DataType.DOUBLE),
                new Attribute("GEOALTITUDE", Attribute.DataType.DOUBLE),
                new Attribute("LASTPOSUPDATE", Attribute.DataType.DOUBLE),
                new Attribute("LASTCONTACT", Attribute.DataType.DOUBLE)
        );



        // Strings
        schema.byName("ICAO24").setMaxStringSize(6);
        schema.byName("CALLSIGN").setMaxStringSize(8);

        // Indexing
        schema.byName("GEOALTITUDE").setProperty("index", "true").setProperty("secondaryIndex", "true");
        schema.byName("BAROALTITUDE").setProperty("index", "true").setProperty("secondaryIndex", "true");
        schema.byName("VELOCITY").setProperty("index", "true").setProperty("secondaryIndex", "true");
        schema.byName("HEADING").setProperty("index", "true").setProperty("secondaryIndex", "true");
        schema.byName("VERTRATE").setProperty("index", "true").setProperty("secondaryIndex", "true");
        return schema;
    }

    private static PrimaryWithSecondaryIndexImpl createTree(Path input, Path directory, ExperimentUtil.ExperimentConfig cfg)
            throws DBException, NumberFormatException, IOException {
        Files.createDirectories(directory);

        PrimaryWithSecondaryIndexImpl result = new PrimaryWithSecondaryIndexImpl(directory, getSchema(),
                TimeRepresentation.POINT, cfg.iParams, cfg.mParams);

        try (var r = Files.newBufferedReader(input)) {
            r.readLine(); // Swallow header

            String line = null;
            while ((line = r.readLine()) != null) {
                String[] values = line.split(";");

                // Strip timestamp
                Object[] payload = new Object[values.length - 1];

                payload[0] = Long.parseLong(values[0]);       // TIME
                payload[1] = values[1];                       // ICAO
                payload[2] = Double.parseDouble(values[2]);   // LAT
                payload[3] = Double.parseDouble(values[3]);   // LON
                payload[4] = Double.parseDouble(values[4]);   // VELOCITY
                payload[5] = Double.parseDouble(values[5]);   // HEADING
                payload[6] = Double.parseDouble(values[6]);   // VERTRATE
                payload[7] = values[7];                       // CALLSIGN
                payload[8] = Byte.parseByte(values[8]);       // ONGROUND
                payload[9] = Byte.parseByte(values[9]);       // ALERT
                payload[10] = Byte.parseByte(values[10]);     // SPI
                payload[11] = Integer.parseInt(values[11]);   // SQUAWK
                payload[12] = Double.parseDouble(values[12]); // BAROALT
                payload[13] = Double.parseDouble(values[13]); // GEOALT
                payload[14] = Double.parseDouble(values[14]); // LASTPOSUPDATE
                payload[15] = Double.parseDouble(values[15]); // LASTCONTACT
                result.insert(new SimpleEvent(payload, Long.parseLong(values[16])));
            }
        }
        return result;
    }


    public static Pair<AccessCountingPrimaryIndex, List<AccessCountingLSMTimeIndex<?>>> getTree(ExperimentUtil.ExperimentConfig cfg) throws NumberFormatException, DBException, IOException {

        Path dest = cfg.basePath.resolve("db").resolve("flight");

        if (!Files.exists(dest)) {
            System.out.println("Creating tree");
            Path src = cfg.basePath.resolve("src").resolve("flight.csv");
            createTree(src,dest,cfg).close();
            System.out.println("Finished creating tree");
        }

        System.out.println("Loading tree and secondary indexes");

        AccessCountingPrimaryIndex primary = new AccessCountingPrimaryIndex(dest, cfg.mParams);
        List<AccessCountingLSMTimeIndex<?>> secondaries = new ArrayList<>();

        for (Attribute a : primary.getSchema()) {
            if (a.getProperty("secondaryIndex") != null && a.getProperty("secondaryIndex").equalsIgnoreCase("true")) {
                secondaries.add(new AccessCountingLSMTimeIndex<>(dest, primary.getSchema(), a.getName(),
                        primary.getTree().BLOCK_SIZE,
                        cfg.mParams.isUseDirectIO()));
            }
        }

        return new Pair<>(primary, secondaries);
    }


    private static final PatternMatcher getQuery() {
        long window = TimeUnit.MINUTES.toMillis(15);

        Symbols ss = new Symbols(
                new Symbol('S', new And(
                        new GreaterEq(new Variable("VELOCITY"), new DoubleConstant(140.0)),
                        new GreaterEq(new Variable("GEOALTITUDE"), new DoubleConstant(500.0))
                ),
                        new Bindings()
                ),
                new Symbol('W', new And(
//				new Less(new Variable("VELOCITY"), new DoubleConstant(56.0)),
                        new Less(new Variable("VELOCITY"), new DoubleConstant(140.0)),
                        new Less(new Variable("VERTRATE"), new DoubleConstant(0.0))
                ),
                        new Bindings(
                                new Binding("START_A", new Variable("BAROALTITUDE")),
                                new Binding("START_V", new Variable("VELOCITY"))
                        )
                ),
                new Symbol('Y', new And(
                        new GreaterEq(new PrevVariable("VELOCITY", 1), new Variable("VELOCITY")),
                        new GreaterEq(new PrevVariable("BAROALTITUDE", 1), new Variable("BAROALTITUDE"))
                ),
                        new Bindings()
                ),
                new Symbol('Z', new And(
                        new And(
                                new Less(new Variable("VELOCITY"), new DoubleConstant(83.0)),
                                new Less(new Variable("GEOALTITUDE"), new DoubleConstant(500.0))
                        ),
                        new And(
                                new Less(new Variable("BAROALTITUDE"), new Multiply(new Variable("START_A"), new DoubleConstant(0.5))),
                                new LessEq(new Variable("VELOCITY"), new PrevVariable("VELOCITY", 1)))),
                        new Bindings()));

        Pattern p = new Sequence(
                new Atom('S'),
                new Atom('W'),
                new KleenePlus(new Atom('Y')),
                new Atom('Z'));

        Output out = new Output("START_A", "START_V");

        Stream in = new Stream("flight");
        return new PatternMatcher(in, window, ss, p, out);
    }

    private static void runPattern(AccessCountingPrimaryIndex src, List<AccessCountingLSMTimeIndex<?>> idxList, TransformedPattern pattern, PatternStats stats, int estimationLevel) throws IncompatibleTypeException, TranslatorException {

        if (estimationLevel > 0) {
            var hStats = new HistogramScanner(pattern).estimate(src, estimationLevel);

            List<PatternStats.ConditionStats> wrappedStats = new ArrayList<>();
            for (int i = 0; i < hStats.getConditionStats().size(); i++) {
                wrappedStats.add(new ConditionStatsWrapper(hStats.getConditionStats().get(i), stats.getConditionStats().get(i).getGlobalSelectivity()));
            }
            stats = new PatternStats() {
                @Override
                public List<MatchInterval> getMatchIntervals() {
                    return hStats.getMatchIntervals();
                }

                @Override
                public List<ConditionStats> getConditionStats() {
                    return wrappedStats;
                }
            };

        }

        ExperimentsBasics.IOMeasures iom = new ExperimentsBasics.IOMeasures();
        List<Long> execTimes = new ArrayList<>();

		NaiveStrategySelector nss = new NaiveStrategySelector(src,idxList,pattern,stats);

		var strategies = nss.getAllStrategies().stream().sorted().collect(Collectors.toCollection(ArrayList::new));

		for ( var es : strategies ) {
			iom.resetMeasures(src,idxList);
			long time = -System.currentTimeMillis();
			long results = ExperimentsBasics.consume(es.execute());
			time += System.currentTimeMillis();
			iom.updateAndReset(es, src, idxList);
			execTimes.add(time);
		}
		System.out.println(iom);
		System.out.println("Execution Times: " + execTimes);

    }

    /**
     * @param args
     * @throws ExecutionException
     * @throws InterruptedException
     * @throws IOException
     * @throws DBException
     * @throws IncompatibleTypeException
     * @throws TranslatorException
     */
    public static void main(String[] args) throws DBException, IOException, InterruptedException,
            ExecutionException, TranslatorException, IncompatibleTypeException {
        var config = ExperimentUtil.getConfig();

        var p = getTree(config);

        try {
            var primary = p.getElement1();
            var secondaries = p.getElement2();

            System.out.println("Container compression ratio: " + primary.getCompressionRatio());
            System.out.println("Event size : " + primary.getSerializedEventSize());
            System.out.println("Event count: " + primary.getNumberOfEvents());
            System.out.println("Uncomressed size: " + primary.getSerializedEventSize() * primary.getNumberOfEvents());
            System.out.println("Tree Height: " + primary.getHeight());

            System.out.println("Avg dT: " + (primary.getCoveredTimeInterval().getDuration() / (primary.getNumberOfEvents() - 1)));

            // Warm Up os' page-cache
            System.out.print("Warming up page cache...");
            System.out.println(" Done! (" + ExperimentsBasics.warmUpPageCache(primary) + ")");

            TransformedPattern tp = Util.transformPattern(getQuery(), primary.getSchema());
            PatternStats stats = ExperimentsBasics.computePatternStats(primary, secondaries, tp);

            for (int i = 0; i <= 3; i++) {
                for (int j = 0; j < 10; j++) {
                    System.out.println("=============================================");
                    System.out.println("Histogram level: " + i);
                    runPattern(primary, secondaries, tp, stats, i);
                }
            }
        } finally {
            if (p != null) {
                p.getElement1().close();
                p.getElement2().forEach(AccessCountingLSMTimeIndex::close);
            }
        }

    }

    static class ConditionStatsWrapper implements PatternStats.ConditionStats {

        private final PatternStats.ConditionStats src;
        private final double globalSelectivity;

        public ConditionStatsWrapper(PatternStats.ConditionStats src, double trueSelectivity) {
            this.src = src;
            this.globalSelectivity = trueSelectivity;
        }

        @Override
        public double getGlobalSelectivity() {
            return globalSelectivity;
        }

        @Override
        public SubPatternCondition.ConditionId getConditionId() {
            return src.getConditionId();
        }

        @Override
        public long getTotalEventCount() {
            return src.getTotalEventCount();
        }

        @Override
        public long getTotalDuration() {
            return src.getTotalDuration();
        }

        @Override
        public int getNumIntervals() {
            return src.getNumIntervals();
        }

        @Override
        public double getSelectedEventCount() {
            return src.getSelectedEventCount();
        }

        @Override
        public Iterator<TimeInterval> intervals() {
            return src.intervals();
        }

        @Override
        public double getLocalSelectivity() {
            return src.getLocalSelectivity();
        }
    }

}
