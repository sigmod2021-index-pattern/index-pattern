package sigmod2021.esp.api.expression.spatial.temporal;

import com.vividsolutions.jts.geom.*;

import java.util.Iterator;

public class TemporalGeometry extends Geometry {

    private Geometry geometry;
    private long[] timestamps;

    public TemporalGeometry(Geometry geometry, long[] timestamps, GeometryFactory factory) {
        super(factory);
        this.geometry = geometry;
        this.timestamps = timestamps;
    }

    @Override
    public String getGeometryType() {
        return geometry.getGeometryType();
    }

    @Override
    public Coordinate getCoordinate() {
        return geometry.getCoordinate();
    }

    @Override
    public Coordinate[] getCoordinates() {
        return geometry.getCoordinates();
    }

    @Override
    public int getNumPoints() {
        return geometry.getNumPoints();
    }

    @Override
    public boolean isEmpty() {
        return geometry.isEmpty();
    }

    @Override
    public int getDimension() {
        return geometry.getDimension();
    }

    @Override
    public Geometry getBoundary() {
        return geometry.getBoundary();
    }

    @Override
    public int getBoundaryDimension() {
        return geometry.getBoundaryDimension();
    }

    @Override
    public Geometry reverse() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean equalsExact(Geometry geometry, double v) {
        return geometry.equalsExact(geometry, v);
    }

    @Override
    public void apply(CoordinateFilter coordinateFilter) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void apply(CoordinateSequenceFilter coordinateSequenceFilter) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void apply(GeometryFilter geometryFilter) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void apply(GeometryComponentFilter geometryComponentFilter) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void normalize() {
        geometry.normalize();
    }

    @Override
    protected Envelope computeEnvelopeInternal() {
        throw new UnsupportedOperationException();
    }

    @Override
    protected int compareToSameClass(Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected int compareToSameClass(Object o, CoordinateSequenceComparator coordinateSequenceComparator) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toText() {
        return geometry.toText();
    }

    public long[] getTimestamps() {
        return timestamps;
    }

    public Iterator<TemporalCoordinate> iterator() {
        return new Iterator<TemporalCoordinate>() {

            private int pointer = 0;

            @Override
            public boolean hasNext() {
                return pointer < getNumPoints();
            }

            @Override
            public TemporalCoordinate next() {
                return new TemporalCoordinate(getCoordinates()[pointer], timestamps[pointer++]);
            }
        };
    }

    @Override
    public String toString() {
        return geometry.toString();
    }

    public class TemporalCoordinate {

        private Coordinate coordinate;
        private long timestamp;

        public TemporalCoordinate(Coordinate coordinate, long timestamp) {
            this.coordinate = coordinate;
            this.timestamp = timestamp;
        }

        public Coordinate getCoordinate() {
            return coordinate;
        }

        public long getTimestamp() {
            return timestamp;
        }
    }
}
