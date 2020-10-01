package sigmod2021.pattern.replay;


import sigmod2021.common.IncompatibleTypeException;
import sigmod2021.db.core.primaryindex.PrimaryIndex;
import sigmod2021.db.event.PersistentEvent;
import sigmod2021.db.util.TimeInterval;
import sigmod2021.esp.api.bridge.EventChannel;
import sigmod2021.esp.api.epa.PatternMatcher;
import sigmod2021.esp.bridges.nat.epa.NativeOperator;
import sigmod2021.esp.bridges.nat.epa.impl.pattern.NativePatternMatcher;
import sigmod2021.esp.ql.TranslatorException;
import sigmod2021.event.Event;
import sigmod2021.event.SchemaException;
import xxl.core.cursor.MinimalCursor;
import xxl.core.cursors.Cursor;
import xxl.core.cursors.sources.EmptyCursor;

import java.util.NoSuchElementException;

/**
 * Step-wise pattern matcher for EventStore.
 */
public class ReplayPatternMatcher extends NativeReplayOperator<PatternMatcher> {

    /**
     * Creates a new pattern matcher for the given pattern definition.
     *
     * @param pm the pattern definition
     * @throws IncompatibleTypeException
     * @throws TranslatorException
     */
    public ReplayPatternMatcher(PrimaryIndex tree, PatternMatcher pm) throws TranslatorException, IncompatibleTypeException {
        super(tree, pm, new Fact());
    }

    public Cursor<Event> executeMultiRegions(final Cursor<TimeInterval> intervals) {
        final Cursor<Cursor<PersistentEvent>> regions = tree.query(intervals);
        return new MinimalCursor<Event>() {

            long intervalCount = 0L;

            Cursor<Event> currentResults = new EmptyCursor<>();

            Event next;

            @Override
            public void open() {
                regions.open();
                next = computeNext();
            }

            @Override
            public void close() {
                regions.close();
                currentResults.close();
                System.out.println("Intervals: " + intervalCount);
            }

            @Override
            public boolean hasNext() throws IllegalStateException {
                return next != null;
            }

            @Override
            public Event next() throws IllegalStateException, NoSuchElementException {
                Event result = next;
                next = computeNext();
                return result;
            }

            private Event computeNext() {
                if (currentResults.hasNext())
                    return currentResults.next();

                while (!currentResults.hasNext() && regions.hasNext()) {
                    currentResults.close();
                    currentResults = executeDirect(regions.next());
                    intervalCount++;
                    currentResults.open();
                }

                return currentResults.hasNext() ? currentResults.next() : null;
            }
        };
    }

    static class Fact implements NativeOperatorFactory<PatternMatcher> {

        private NativePatternMatcher instance;

        /**
         * @{inheritDoc}
         */
        @Override
        public NativeOperator create(PatternMatcher def, EventChannel... inputChannels) {
            if (instance == null) {
                try {
                    instance = new NativePatternMatcher(def, inputChannels[0]);
                } catch (SchemaException | IncompatibleTypeException | TranslatorException e) {
                    throw new RuntimeException(e);
                }
            }
            instance.clearState();
            return instance;
        }
    }

//	public Cursor<Event> executeMultiRegions(final Cursor<TimeInterval> intervals) {
//		return new MinimalCursor<Event>() {
//
//			long intervalCount = 0L;
//
//			Cursor<Event> currentResults = new EmptyCursor<>();
//
//			Event next;
//
//			@Override
//			public void open() {
//				intervals.open();
//				next = computeNext();
//			}
//
//			@Override
//			public void close() {
//				intervals.close();
//				currentResults.close();
//				System.err.println("Intervals: " + intervalCount);
//			}
//
//			@Override
//			public boolean hasNext() throws IllegalStateException {
//				return next != null;
//			}
//
//			@Override
//			public Event next() throws IllegalStateException, NoSuchElementException {
//				Event result = next;
//				next = computeNext();
//				return result;
//			}
//
//			private Event computeNext() {
//				if ( currentResults.hasNext() )
//					return currentResults.next();
//
//				while ( !currentResults.hasNext() && intervals.hasNext()  ) {
//					currentResults.close();
//					currentResults = execute(intervals.next());
//					intervalCount++;
//					currentResults.open();
//				}
//
//				return currentResults.hasNext() ? currentResults.next() : null;
//			}
//		};
//	}


    // ============================================================================
}
