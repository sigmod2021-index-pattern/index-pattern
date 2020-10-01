package sigmod2021.esp.api.expression.spatial.temporal;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LineString;

public class TemporalGeometryFactory extends GeometryFactory {

    public TemporalGeometry createTemporalLineString(Coordinate[] coordinates, long[] timestamps) {
        if (coordinates.length != timestamps.length)
            throw new UnsupportedOperationException("Number of coordinates and timestamps does not match");
        LineString lineString = createLineString(coordinates != null ? this.getCoordinateSequenceFactory().create(coordinates) : null);
        return new TemporalGeometry(lineString, timestamps, this);
    }

}
