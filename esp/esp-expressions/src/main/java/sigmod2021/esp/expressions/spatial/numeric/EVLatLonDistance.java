package sigmod2021.esp.expressions.spatial.numeric;

import com.vividsolutions.jts.geom.Geometry;
import sigmod2021.esp.expressions.arithmetic.EVSpatialExpression;
import sigmod2021.event.Attribute.DataType;

/**
 *  Calculates the distance in meters between the two input geometries with lat/lon coordinates.
 *
 *
 */
public class EVLatLonDistance extends EVBinarySpatialNumericExpression {

    private static final double EARTH_RADIUS = 6_378_137; // RADIUS in m

    public EVLatLonDistance(EVSpatialExpression left, EVSpatialExpression right) {
        super(left, right, DataType.DOUBLE, (Geometry x, Geometry y) -> {
            double lon1 = x.getCoordinate().x;
            double lat1 = x.getCoordinate().y;

            double lon2 = y.getCoordinate().x;
            double lat2 = y.getCoordinate().y;

            double dLat = Math.toRadians(lat2 - lat1);
            double dLon = Math.toRadians(lon2 - lon1);

            double a = Math.sin(dLat / 2) * Math.sin(dLat / 2) +
                    Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2)) *
                            Math.sin(dLon / 2) * Math.sin(dLon / 2);

            double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));

            return EARTH_RADIUS * c;
        });
    }
}
