package sigmod2021.pattern.util;

import sigmod2021.db.core.secondaryindex.EventID;
import xxl.core.cursors.AbstractCursor;
import xxl.core.cursors.Cursor;
import xxl.core.util.Pair;

import java.util.*;

public class TIDCursor extends AbstractCursor<EventID> {

    static final Random rand = new Random(1337L);

    final Cursor<Pair<?, EventID>> siQueryResult;

    Iterator<EventID> ids;

    /**
     * Creates a new TIDCursor instance
     * @param siQueryResult
     */
    public TIDCursor(Cursor<Pair<?, EventID>> siQueryResult) {
        this.siQueryResult = siQueryResult;
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public void open() {
        if (isOpened)
            return;

        super.open();

        List<EventID> tmp = new ArrayList<>();

        long time = -System.nanoTime();
        siQueryResult.open();
        siQueryResult.forEachRemaining(p -> tmp.add(p.getElement2()));
        siQueryResult.close();
        time += System.nanoTime();

        if (PMTiming.DO_TIMING) {
            PMTiming.getCurrentTiming().addSecondaryReadNanos(time);
        }
        Collections.shuffle(tmp, rand);

//		System.err.println("Readout: " + (System.currentTimeMillis()+time) + " ms");
        time = -System.nanoTime();
        Collections.sort(tmp);
        time += System.nanoTime();

        if (PMTiming.DO_TIMING) {
            PMTiming.getCurrentTiming().addSecondarySortNanos(time);
        }

//		time += System.currentTimeMillis();
//		System.err.println("SI access took: " + time + " ms (" + tmp.size() + " elements)");
        ids = tmp.iterator();
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public void close() {
        super.close();
    }

    /**
     * @{inheritDoc}
     */
    @Override
    protected boolean hasNextObject() {
        return ids.hasNext();
    }

    /**
     * @{inheritDoc}
     */
    @Override
    protected EventID nextObject() {
        return ids.next();
    }


}
