package sigmod2021.esp.api.util;

import sigmod2021.event.Event;

/**
 * Utilities for working with events
 */
public class EventUtil {

    /**
     * Compares two events for equality, using the given delta for floating point comparisons
     *
     * @param e1      the first event
     * @param e2      the second event
     * @param fpdelta the delta to use for floating point comparisons
     * @return true if both events are equal, false otherwise
     */
    public static final boolean compareEvents(Event e1, Event e2, double fpdelta) {
        if (e1.getNumberOfAttributes() != e2.getNumberOfAttributes())
            return false;
        if (e1.getT1() != e2.getT1())
            return false;
        if (e1.getT2() != e2.getT2())
            return false;

        for (int i = 0; i < e1.getNumberOfAttributes(); i++) {
            if (!e1.get(i).getClass().equals(e2.get(i).getClass())) {
                System.out.println(e1.get(i).getClass());
                System.out.println(e2.get(i).getClass());
                return false;
            }


            if (e1.get(i) instanceof Float
                    && Math.abs(e1.get(i, Float.class) - e2.get(i, Float.class)) > (float) fpdelta)
                return false;
            else if (e1.get(i) instanceof Double
                    && Math.abs(e1.get(i, Double.class) - e2.get(i, Double.class)) > fpdelta)
                return false;
            else if (!e1.get(i).equals(e2.get(i)))
                return false;
        }
        return true;
    }
}
