package sigmod2021.pattern.experiments.data;

import sigmod2021.event.Attribute;
import sigmod2021.event.Attribute.DataType;
import sigmod2021.event.Event;
import sigmod2021.event.EventSchema;
import sigmod2021.event.impl.SimpleEvent;

import java.util.Iterator;
import java.util.Random;

/**
 *
 */
public class MultiDataGenerator implements DataSource {

    private final long numEvents;
    private final double stepWidth;
    private final Distorter[] distorters;
    private final EventSchema schema;

    private static final long SEED = 1337;
    private static final Random RAND = new Random(SEED);

    public static final Distorter[] DISTRIBS = {

            new MultiDataGenerator.NoDistorter(),  // Straight line 0-1
            new MultiDataGenerator.GaussianDistorter(RAND, 0.001),  // Straight line 0-1, distored by gaussion with stddev= 0.001
            new MultiDataGenerator.GaussianDistorter(RAND, 0.01),  // Straight line 0-1, distored by gaussion with stddev= 0.01
            new MultiDataGenerator.GaussianDistorter(RAND, 0.1),  // Straight line 0-1, distored by gaussion with stddev= 0.1
            new MultiDataGenerator.GaussianDistorter(RAND, 1.0),  // Straight line 0-1, distored by gaussion with stddev= 1

            new MultiDataGenerator.ReverseDistorter(),  // Straight line 1-0
            new MultiDataGenerator.CombinedDistorter(new MultiDataGenerator.ReverseDistorter(),
                    new MultiDataGenerator.GaussianDistorter(RAND, 0.001)),  // Straight line 1-0, distored by gaussion with stddev= 0.001
            new MultiDataGenerator.CombinedDistorter(new MultiDataGenerator.ReverseDistorter(),
                    new MultiDataGenerator.GaussianDistorter(RAND, 0.01)),  // Straight line 1-0, distored by gaussion with stddev= 0.01
            new MultiDataGenerator.CombinedDistorter(new MultiDataGenerator.ReverseDistorter(),
                    new MultiDataGenerator.GaussianDistorter(RAND, 0.1)),  // Straight line 1-0, distored by gaussion with stddev= 0.1
            new MultiDataGenerator.CombinedDistorter(new MultiDataGenerator.ReverseDistorter(),
                    new MultiDataGenerator.GaussianDistorter(RAND, 1.0)),  // Straight line 1-0, distored by gaussion with stddev= 1

            new MultiDataGenerator.UpDownDistorter(),  // Straight line 0-1-0
            new MultiDataGenerator.CombinedDistorter(new MultiDataGenerator.UpDownDistorter(),
                    new MultiDataGenerator.GaussianDistorter(RAND, 0.001)),  // Straight line 0-1-0, distored by gaussion with stddev= 0.001
            new MultiDataGenerator.CombinedDistorter(new MultiDataGenerator.UpDownDistorter(),
                    new MultiDataGenerator.GaussianDistorter(RAND, 0.01)),  // Straight line 0-1-0, distored by gaussion with stddev= 0.01
            new MultiDataGenerator.CombinedDistorter(new MultiDataGenerator.UpDownDistorter(),
                    new MultiDataGenerator.GaussianDistorter(RAND, 0.1)),  // Straight line 0-1-0, distored by gaussion with stddev= 0.1
            new MultiDataGenerator.CombinedDistorter(new MultiDataGenerator.UpDownDistorter(),
                    new MultiDataGenerator.GaussianDistorter(RAND, 1.0)),  // Straight line 0-1-0, distored by gaussion with stddev= 1
    };

    public MultiDataGenerator(long numEvents) {
        this( numEvents, DISTRIBS );
    }


    /**
     * Creates a new MultiDataGenerator instance
     */
    public MultiDataGenerator(long numEvents, Distorter... distorters) {
        this.distorters = distorters;

        Attribute[] schema = new Attribute[distorters.length];
        for (int i = 0; i < schema.length; i++)
            schema[i] = new Attribute("A" + i, DataType.DOUBLE);

        this.numEvents = numEvents;
        this.stepWidth = 1.0 / numEvents;
        this.schema = new EventSchema(schema);
    }

    public static void main(String[] args) {
        Random RAND = new Random(1337);
        MultiDataGenerator mdg = new MultiDataGenerator(1000,
                new NoDistorter(),
                new GaussianDistorter(RAND, 0.001),
                new GaussianDistorter(RAND, 0.01),
                new GaussianDistorter(RAND, 0.1),
                new GaussianDistorter(RAND, 1));

        double[][] values = new double[5][1000];

        int pos = 0;
        for (Iterator<Event> iter = mdg.iterator(); iter.hasNext(); pos++) {
            Event e = iter.next();
            for (int i = 0; i < values.length; i++) {
                values[i][pos] = e.get(i, Double.class);
            }
        }

        for (int i = 0; i < values.length; i++) {
            for (double v : values[i])
                System.out.print(v + ";");
            System.out.println();
        }

    }

    /**
     * @{inheritDoc}
     */
    @Override
    public EventSchema getSchema() {
        return schema;
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public Iterator<Event> iterator() {
        return new Iterator<Event>() {

            long timestamp = 1L;

            private double last = -stepWidth;

            @Override
            public boolean hasNext() {
                return timestamp <= numEvents;
            }

            private Object[] nextValue() {
                last += stepWidth;

                Object[] result = new Object[distorters.length];
                for (int i = 0; i < distorters.length; i++) {
                    result[i] = distorters[i].nextValue(last);
                }
                return result;
            }

            @Override
            public Event next() {
                Object[] payload = nextValue();
                return new SimpleEvent(payload, timestamp++);
            }
        };
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public String getName() {
        return "SuperMultiStream";
    }

    public static interface Distorter {

        double nextValue(double linearValue);
    }

    public static class NoDistorter implements Distorter {

        @Override
        public double nextValue(double linearValue) {
            return linearValue;
        }
    }

    public static class GaussianDistorter implements Distorter {

        private final Random rand;

        private final double stdDev;

        /**
         * Creates a new GaussianDistorter instance
         * @param rand
         * @param stdDev
         */
        public GaussianDistorter(Random rand, double stdDev) {
            this.rand = rand;
            this.stdDev = stdDev;
        }

        /**
         * @{inheritDoc}
         */
        @Override
        public double nextValue(double linearValue) {
            double value = 0.0;

            do {
                value = (rand.nextGaussian() * stdDev) + linearValue;
            } while (value < 0 || value > 1);
            return value;
        }
    }

    public static class ReverseDistorter implements Distorter {
        @Override
        public double nextValue(double linearValue) {
            return 1.0 - linearValue;
        }
    }

    public static class UpDownDistorter implements Distorter {
        @Override
        public double nextValue(double linearValue) {
            if (linearValue * 2 > 1)
                return 2.0 - 2 * linearValue;
            else
                return 2 * linearValue;
        }
    }

    public static class CombinedDistorter implements Distorter {

        private final Distorter fst;

        private final Distorter snd;

        /**
         * Creates a new CombinedDistorter instance
         * @param fst
         * @param snd
         */
        public CombinedDistorter(Distorter fst, Distorter snd) {
            this.fst = fst;
            this.snd = snd;
        }

        /**
         * @{inheritDoc}
         */
        @Override
        public double nextValue(double linearValue) {
            return snd.nextValue(fst.nextValue(linearValue));
        }
    }

}
