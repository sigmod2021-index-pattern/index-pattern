package sigmod2021.esp.bridges.nat.epa.impl.aggregator.aggregates;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LineString;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 *
 */
public class TrajectoryFunction implements AggregateFunction<List<Coordinate>, LineString, Geometry> {

    /** The serialVersionUID */
    private static final long serialVersionUID = 1L;

    private static final GeometryFactory GF = new GeometryFactory();

    /**
     * @{inheritDoc}
     */
    @Override
    public List<Coordinate> fInit() {
        return Collections.emptyList();
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public List<Coordinate> fInit(Geometry initialValue) {
        return Collections.singletonList(initialValue.getCentroid().getCoordinate());
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public List<Coordinate> fMerge(List<Coordinate> left, List<Coordinate> right) {
        List<Coordinate> result = new ArrayList<>(left);
        result.addAll(right);
        return result;
    }

    /**
     * @{inheritDoc}
     */
    @Override
    public LineString fEval(List<Coordinate> value, long counter) {
        if (value.size() == 1) {
            Coordinate c = value.get(0);
            return GF.createLineString(new Coordinate[]{c, c});
        } else
            return GF.createLineString(value.toArray(new Coordinate[value.size()]));
    }
}
