package xxl.core.cursor;

import java.util.Iterator;

import xxl.core.cursors.DecoratorCursor;

/**
 * Created by seidemann on 15.12.2016.
 */
public class DoubleCursors {

    public static <E> DoubleCursor<E> wrap(Iterator<E> cursor) {
        if (cursor instanceof DoubleCursor)
            return (DoubleCursor<E>)cursor;
        else return new CursorDoubleCursor<>(cursor);
    }

    private static class CursorDoubleCursor<E> extends DecoratorCursor<E> implements DoubleCursor<E> {
        public CursorDoubleCursor(Iterator<E> iterator) {
            super(iterator);
        }

        @Override
        public boolean supportsPeekBack() {
            return false;
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
}
