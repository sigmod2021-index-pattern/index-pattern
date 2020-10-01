package sigmod2021.db.core.wal;

/**
 * Empy implementation of a log.
 */
public class NoLog<T> implements Log<T> {

    @Override
    public void insert(final long blockID, final T item) {
    }

    @Override
    public long getLSN() {
        return 0;
    }

    @Override
    public void clear() {
    }
}
