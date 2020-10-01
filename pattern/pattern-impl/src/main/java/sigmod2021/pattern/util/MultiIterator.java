package sigmod2021.pattern.util;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * An iterator wrapping any number of iterators and iterates them sequentially.
 */
public class MultiIterator<T> implements Iterator<T> {

    /** All remaining iterators */
    private Iterator<Iterator<T>> main;

    /** The current iterator */
    private Iterator<T> current;

    /**
     * @param inputs the iterators to wrap
     */
    public MultiIterator(List<Iterator<T>> inputs) {
        main = inputs.iterator();
        current = main.next();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean hasNext() {
        return current.hasNext() || main.hasNext();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public T next() {
        try {
            return current.next();
        } catch (NoSuchElementException nse) {
            current = main.next();
            return current.next();
        }
    }
}
