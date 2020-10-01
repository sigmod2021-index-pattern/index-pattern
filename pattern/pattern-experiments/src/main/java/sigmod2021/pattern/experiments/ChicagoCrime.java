package sigmod2021.pattern.experiments;

import sigmod2021.common.IncompatibleTypeException;
import sigmod2021.db.DBException;
import sigmod2021.db.core.primaryindex.impl.PrimaryWithSecondaryIndexImpl;
import sigmod2021.db.core.primaryindex.queries.range.DoubleAttributeRange;
import sigmod2021.pattern.cost.execution.ExecutionStrategy;
import sigmod2021.pattern.cost.selection.PatternStats;
import sigmod2021.pattern.cost.selection.impl.NaiveStrategySelector;
import sigmod2021.pattern.cost.transform.TransformedPattern;
import sigmod2021.pattern.experiments.util.ExperimentsBasics;
import sigmod2021.pattern.util.Util;
import sigmod2021.pattern.experiments.util.ExperimentUtil;
import sigmod2021.pattern.experiments.util.AccessCountingLSMTimeIndex;
import sigmod2021.pattern.experiments.util.AccessCountingPrimaryIndex;
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
import sigmod2021.esp.api.expression.BooleanExpression;
import sigmod2021.esp.api.expression.arithmetic.atomic.DoubleConstant;
import sigmod2021.esp.api.expression.arithmetic.atomic.IntConstant;
import sigmod2021.esp.api.expression.arithmetic.atomic.ShortConstant;
import sigmod2021.esp.api.expression.arithmetic.atomic.Variable;
import sigmod2021.esp.api.expression.logical.And;
import sigmod2021.esp.api.expression.logical.True;
import sigmod2021.esp.api.expression.predicate.Between;
import sigmod2021.esp.api.expression.predicate.Equal;
import sigmod2021.esp.ql.TranslatorException;
import sigmod2021.event.Attribute;
import sigmod2021.event.Attribute.DataType;
import sigmod2021.event.EventSchema;
import sigmod2021.event.TimeRepresentation;
import sigmod2021.event.impl.SimpleEvent;
import xxl.core.util.Pair;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;

/**
 *
 */
public class ChicagoCrime {

    private static EventSchema getSchema() {

        EventSchema schema = new EventSchema(
                //			new Attribute("seq", DataType.LONG),
                new Attribute("id", DataType.LONG),
                new Attribute("case", DataType.STRING),
                //			new Attribute("date", DataType.STRING),
                new Attribute("block", DataType.STRING),
                new Attribute("IUCR", DataType.STRING),
                new Attribute("Primary", DataType.INTEGER),
                new Attribute("Description", DataType.STRING),
                new Attribute("Location_Desc", DataType.STRING),
                new Attribute("Arrest", DataType.BYTE), // BOOL
                new Attribute("Domestic", DataType.BYTE), // BOOL
                new Attribute("Beat", DataType.SHORT),
                new Attribute("District", DataType.FLOAT),
                new Attribute("Ward", DataType.FLOAT),
                new Attribute("Community", DataType.FLOAT),
                new Attribute("FBI", DataType.STRING),
                new Attribute("X", DataType.DOUBLE),
                new Attribute("Y", DataType.DOUBLE),
                new Attribute("Year", DataType.SHORT),
                new Attribute("Updated", DataType.STRING),
                new Attribute("Latitude", DataType.DOUBLE),
                new Attribute("Longitude", DataType.DOUBLE));


        // Strings
        schema.byName("case").setMaxStringSize(9);
        schema.byName("block").setMaxStringSize(64); // 36
        schema.byName("IUCR").setMaxStringSize(4);
        schema.byName("Primary").setMaxStringSize(64); // 33
        schema.byName("Description").setMaxStringSize(64); // 60
        schema.byName("Location_Desc").setMaxStringSize(64); // 53
        schema.byName("FBI").setMaxStringSize(3);
        schema.byName("Updated").setMaxStringSize(22);

        // Indexing
        schema.byName("Beat").setProperty("index", "true").setProperty("secondaryIndex", "true");
        schema.byName("Latitude").setProperty("index", "true").setProperty("secondaryIndex", "true");
        schema.byName("Longitude").setProperty("index", "true").setProperty("secondaryIndex", "true");
        schema.byName("Primary").setProperty("index", "true").setProperty("secondaryIndex", "true");
        return schema;
    }

    private static PrimaryWithSecondaryIndexImpl createTree(Path input, Path directory, ExperimentUtil.ExperimentConfig cfg)
            throws DBException, NumberFormatException, IOException {
        Files.createDirectories(directory);

        PrimaryWithSecondaryIndexImpl result = new PrimaryWithSecondaryIndexImpl(directory, getSchema(),
                TimeRepresentation.POINT, cfg.iParams, cfg.mParams);

        System.out.println("Building dictionary for primary category.");

        Map<String, Integer> primaryDictionary = new HashMap<>();
        try (var r = Files.newBufferedReader(input)) {
            NavigableSet<String> primary = new TreeSet<>();
            r.readLine(); // Swallow header

            String line = null;
            while ((line = r.readLine()) != null) {
                primary.add(line.split(";")[4].trim().toUpperCase());
            }

            int id = 0;
            for (String p : primary) {
                primaryDictionary.put(p, id++);
            }
        }
        System.out.println("Finished building dictionary for primary category:");
        primaryDictionary.entrySet().stream()
                .sorted(Comparator.comparingInt(Map.Entry::getValue))
                .forEach(x -> System.out.println("  " + x.getValue() + ": " + x.getKey()));


        try (var r = Files.newBufferedReader(input)) {
            r.readLine(); // Swallow header

            String line = null;
            while ((line = r.readLine()) != null) {

                String[] values = line.split(";");

                // Strip timestamp
                Object[] payload = new Object[values.length - 1];

                payload[0] = Long.parseLong(values[0]);
                payload[1] = values[1];
                payload[2] = values[2];
                payload[3] = values[3];
                // Use dictionary
                payload[4] = primaryDictionary.get(values[4].trim().toUpperCase());
                payload[5] = values[5];
                payload[6] = values[6];
                payload[7] = values[7].equalsIgnoreCase("true") ? (byte) 1 : (byte) 0;
                payload[8] = values[8].equalsIgnoreCase("true") ? (byte) 1 : (byte) 0;
                payload[9] = Short.parseShort(values[9]);
                payload[10] = Float.parseFloat(values[10]);
                payload[11] = Float.parseFloat(values[11]);
                payload[12] = Float.parseFloat(values[12]);
                payload[13] = values[13];
                payload[14] = Double.parseDouble(values[14]);
                payload[15] = Double.parseDouble(values[15]);
                payload[16] = Short.parseShort(values[16]);
                payload[17] = values[17];
                payload[18] = Double.parseDouble(values[18]);
                payload[19] = Double.parseDouble(values[19]);
                result.insert(new SimpleEvent(payload, Long.parseLong(values[20])));
            }
        }
        return result;
    }

    public static Pair<AccessCountingPrimaryIndex, List<AccessCountingLSMTimeIndex<?>>> getTree(ExperimentUtil.ExperimentConfig cfg) throws NumberFormatException, DBException, IOException {

        Path dest = cfg.basePath.resolve("db").resolve("crime");

        if (!Files.exists(dest)) {
            System.out.println("Creating tree");
            Path src = cfg.basePath.resolve("src").resolve("Crimes_Ordered.csv");
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

    private static Symbol createCrimeSymbol(char name, Integer primary, int beat) {
        BooleanExpression be = new Equal(new Variable("Primary"), new IntConstant(primary));

        if (beat >= 0)
            be = new And(be, new Equal(new Variable("Beat"), new ShortConstant(beat)));

        Bindings bindings = new Bindings(
                new Binding(name + "_ID", new Variable("ID")),
                new Binding(name + "_PRIMARY", new Variable("Primary")),
                new Binding(name + "_BEAT", new Variable("Beat")),
                new Binding(name + "_LAT", new Variable("Latitude")),
                new Binding(name + "_LON", new Variable("Longitude")),
                new Binding(name + "_LOC_DESC", new Variable("Location_Desc")),
                new Binding(name + "_DESC", new Variable("Description")));

        return new Symbol(name, be, bindings);
    }


    private static Symbol createCrimeSymbolLatLon(char name, Integer primary, DoubleAttributeRange lat, DoubleAttributeRange lon) {
        BooleanExpression primBe = new Equal(new Variable("Primary"), new IntConstant(primary));
        BooleanExpression latBe = new Between(new Variable("Latitude"), new DoubleConstant(lat.getLower()), new DoubleConstant(lat.getUpper()));
        BooleanExpression lonBe = new Between(new Variable("Longitude"), new DoubleConstant(lon.getLower()), new DoubleConstant(lon.getUpper()));


        BooleanExpression be = new And(new And(latBe, lonBe), primBe);

        Bindings bindings = new Bindings(
                new Binding(name + "_ID", new Variable("ID")),
                new Binding(name + "_PRIMARY", new Variable("Primary")),
                new Binding(name + "_BEAT", new Variable("Beat")),
                new Binding(name + "_LAT", new Variable("Latitude")),
                new Binding(name + "_LON", new Variable("Longitude")),
                new Binding(name + "_LOC_DESC", new Variable("Location_Desc")),
                new Binding(name + "_DESC", new Variable("Description")));

        return new Symbol(name, be, bindings);
    }


    private static PatternMatcher getCrimeSequenceQuery(List<Integer> primary, int beat, long window) {

        List<Symbol> ss = new ArrayList<>();
        ss.add(new Symbol('X', new True()));

        char id = 'A';
        for (Integer p : primary) {
            ss.add(createCrimeSymbol(id++, p, beat));
        }

        List<String> out = new ArrayList<>();
        List<Pattern> seq = new ArrayList<>();
        for (int i = 0; i < primary.size(); i++) {
            char n = (char) (i + 'A');
            if (i > 0)
                seq.add(new KleeneStar(new Atom('X')));
            seq.add(new Atom(n));

            out.add(n + "_ID");
            out.add(n + "_PRIMARY");
            out.add(n + "_LAT");
            out.add(n + "_LON");
            out.add(n + "_BEAT");
            out.add(n + "_LOC_DESC");
            out.add(n + "_DESC");
        }

        Pattern p = new Sequence(seq);
        Output o = new Output(out.toArray(new String[0]));
        return new PatternMatcher(new Stream("crimes"), window, new Symbols(ss), p, o);
    }


    private static PatternMatcher getCrimeSequenceLatLonQuery(List<Integer> primary, long window, DoubleAttributeRange lat, DoubleAttributeRange lon) {

        List<Symbol> ss = new ArrayList<>();
        ss.add(new Symbol('X', new True()));

        char id = 'A';
        for (Integer p : primary) {
            ss.add(createCrimeSymbolLatLon(id++, p, lat, lon));
        }

        List<String> out = new ArrayList<>();
        List<Pattern> seq = new ArrayList<>();
        for (int i = 0; i < primary.size(); i++) {
            char n = (char) (i + 'A');
            if (i > 0)
                seq.add(new KleeneStar(new Atom('X')));
            seq.add(new Atom(n));

            out.add(n + "_ID");
            out.add(n + "_PRIMARY");
            out.add(n + "_BEAT");
            out.add(n + "_LAT");
            out.add(n + "_LON");
            out.add(n + "_LOC_DESC");
            out.add(n + "_DESC");
        }

        Pattern p = new Sequence(seq);
        Output o = new Output(out.toArray(new String[0]));
        return new PatternMatcher(new Stream("crimes"), window, new Symbols(ss), p, o);
    }


    private static void executePattern(PatternMatcher def, AccessCountingPrimaryIndex primary,
                                       List<AccessCountingLSMTimeIndex<?>> secondaries) throws TranslatorException, IncompatibleTypeException {

        TransformedPattern pattern = Util.transformPattern(def, primary.getSchema());

        System.out.println("Computing pattern stats");
        PatternStats stats = ExperimentsBasics.computePatternStats(primary, secondaries, pattern);

        NaiveStrategySelector naive = new NaiveStrategySelector(primary, secondaries, pattern, stats);

        System.out.println("Naive selected strategy  : " + naive.selectIndexes().getStrategy());

        ExperimentsBasics.IOMeasures iom = new ExperimentsBasics.IOMeasures();
        List<Long> execTimes = new ArrayList<>();

        var strategies = naive.getAllStrategies().stream().sorted().collect(Collectors.toList());

		// Warm up
		System.out.println("Warming Up");
		for ( var s : strategies.subList(0,Math.min(10,strategies.size())) ) {
			ExperimentsBasics.consume(s.execute());
		}

		System.out.println("Executing based " + strategies.size() + " strategies.");
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
                                        List<AccessCountingLSMTimeIndex<?>> secondaries,
                                        ExecutionStrategy es, ExperimentsBasics.IOMeasures iom) throws IncompatibleTypeException, TranslatorException {
        iom.resetMeasures(primary, secondaries);
        long time = -System.currentTimeMillis();
        ExperimentsBasics.consume(es.execute());
        time += System.currentTimeMillis();
        iom.updateAndReset(es, primary, secondaries);
        return time;
    }

    /**
     * @param args
     * @throws DBException
     * @throws IOException
     * @throws NumberFormatException
     * @throws IncompatibleTypeException
     * @throws TranslatorException
     */
    public static void main(String[] args) throws DBException, NumberFormatException, IOException,
            TranslatorException, IncompatibleTypeException {
        var config = ExperimentUtil.getConfig();

        var p = getTree(config);

        try {
            var primary = p.getElement1();
            System.out.println("Index height is  : " + primary.getHeight());
            System.out.println("Compression ratio: " + primary.getCompressionRatio());

            // Warm Up os' page-cache
            System.out.print("Warming up page cache...");
            System.out.println(" Done! (" + ExperimentsBasics.warmUpPageCache(primary) + ")");

            System.out.println("Executing query with BEAT");

//			PatternMatcher defBeat =
//				getCrimeSequenceQuery(List.of("ROBBERY", "BATTERY", "MOTOR VEHICLE THEFT"), 2232, 30 * 60 * 1000); // 30 minutes

            PatternMatcher defBeat =
                    getCrimeSequenceQuery(List.of(30, 2, 17), 2232, 30 * 60 * 1000); // 30 minutes

            executePattern(defBeat, primary, p.getElement2());

            System.out.println("=============================================");
            System.out.println("Executing query with LAT/LON");

//			PatternMatcher defLatLon =
//				getCrimeSequenceLatLonQuery(List.of("ROBBERY", "BATTERY", "MOTOR VEHICLE THEFT"), 30 * 60 * 1000, // 30 minutes
//					new DoubleAttributeRange("Latitude", 41.6994435912654, 41.7146453168236, true,true),
//					new DoubleAttributeRange("Longitude", -87.6587416716118, -87.6330873595214, true,true));

            PatternMatcher defLatLon =
                    getCrimeSequenceLatLonQuery(List.of(30, 2, 17), 30 * 60 * 1000, // 30 minutes
                            new DoubleAttributeRange("Latitude", 41.6994435912654, 41.7146453168236, true, true),
                            new DoubleAttributeRange("Longitude", -87.6587416716118, -87.6330873595214, true, true));

            executePattern(defLatLon, primary, p.getElement2());
        } finally {
            if (p != null) {
                p.getElement1().close();
                p.getElement2().forEach(AccessCountingLSMTimeIndex::close);
            }
        }
    }

}
