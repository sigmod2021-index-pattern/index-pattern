package sigmod2021.db.core.wal;

/**
 * Abstraction of a log.
 */
public interface Log<T> {

    /**
     * Inserts a new item into the log
     *
     * @param blockID the id of the affected block
     * @param item    the item
     * @return the LSN of the event
     */
    void insert(long blockID, T item);

    /**
     * Returns the last written LSN. If the log is empty, -1 is returned.
     *
     * @return the current LSN
     */
    long getLSN();

    /**
     * Clears the log.
     */
    void clear();
}
