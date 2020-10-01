package sigmod2021.esp.bridges.nat.epa.util.spatial;

import com.vividsolutions.jts.geom.Coordinate;
import sigmod2021.esp.api.expression.spatial.temporal.TemporalGeometry;
import sigmod2021.esp.api.expression.spatial.temporal.TemporalGeometryFactory;

import java.util.*;

public class TemporalGeometryMerger {

    private PriorityQueue<TemporalCoordinateIteratorWrapperTemporalCoordinate> heap;

    public TemporalGeometryMerger(Iterator<TemporalGeometry.TemporalCoordinate>[] iterator) {
        this.heap = new PriorityQueue<>(iterator.length, Comparator.comparingLong(o -> o.next().getTimestamp()));
        for (int i = 0; i < iterator.length; i++) {
            heap.add(new TemporalCoordinateIteratorWrapperTemporalCoordinate(iterator[i]));
        }
    }

    public TemporalGeometry merge() {
        List<Coordinate> coordinates = new ArrayList<>();
        List<Long> timestamps = new ArrayList<>();
        while (!heap.isEmpty()) {
            TemporalCoordinateIteratorWrapperTemporalCoordinate poll = heap.poll();
            TemporalGeometry.TemporalCoordinate next = poll.next();
            coordinates.add(next.getCoordinate());
            timestamps.add(next.getTimestamp());
            poll.progress();
            if (poll.next() != null) {
                heap.add(poll);
            }
        }
        return new TemporalGeometryFactory().createTemporalLineString(
                coordinates.toArray(new Coordinate[coordinates.size()]),
                timestamps.stream().mapToLong(Long::longValue).toArray()
        );
    }

    class TemporalCoordinateIteratorWrapperTemporalCoordinate {
        private TemporalGeometry.TemporalCoordinate next;
        private Iterator<TemporalGeometry.TemporalCoordinate> iterator;

        public TemporalCoordinateIteratorWrapperTemporalCoordinate(Iterator<TemporalGeometry.TemporalCoordinate> iterator) {
            this.iterator = iterator;
        }

        public void progress() {
            if (iterator.hasNext())
                next = iterator.next();
            else
                next = null;
        }

        public TemporalGeometry.TemporalCoordinate next() {
            if (next != null)
                return next;
            else if (!iterator.hasNext())
                return null;
            progress();
            return next();
        }

    }

}
