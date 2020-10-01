package sigmod2021.db.core;


/**
 *
 */
public interface IOMeasuring {

    public static <T extends Number> long formatMiB(T value) {
        return value.longValue() / 1024 / 1024;
    }

    void emptyBuffers();

    void resetMeasurement();

    long getTotalReadBytes();

    long getTotalWrittenBytes();
}
