package xxl.core.cursor;

import xxl.core.cursors.sources.EmptyCursor;

/**
 * Empty double cursor.
 *
 */
public class EmptyDoubleCursor<E> extends EmptyCursor<E> implements DoubleCursor<E> {

    @Override
    public boolean supportsPeekBack() {
        return true;
    }

    @Override
    public E peekBack() {
        return null;
    }

    @Override
    public E previous() {
        return null;
    }

    @Override
    public boolean hasPrevious() {
        return false;
    }
}
