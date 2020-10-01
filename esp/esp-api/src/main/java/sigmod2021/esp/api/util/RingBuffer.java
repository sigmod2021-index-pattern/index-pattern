package sigmod2021.esp.api.util;

import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * A simple ring-buffer assuming the elements are inserted in a specific order.
 * Thus this buffer provides methods for efficient searching of elements.
 *
 */
public class RingBuffer<T> {

    /** The array storing the actual elements -- capacity is alway a power of 2 */
    protected Object[] elements = new Object[16];
    /** Pointer to the index of the first element */
    protected int head = 0;
    /** Bit-mask used to wrap logical indexes to physical ones */
    protected int mask = elements.length - 1;
    /** The number of modifications */
    private int modcount = 0;
    /** Pointer to the next insertion index */
    private int tail = 0;

    /**
     * Doubles the capacity of this buffer
     */
    private void doubleCapacity() {
        int p = head;
        int n = elements.length;
        int r = n - p; // number of elements to the right of p
        int newCapacity = n << 1;
        if (newCapacity < 0)
            throw new IllegalStateException("Sorry, buffer too big");
        Object[] a = new Object[newCapacity];
        System.arraycopy(elements, p, a, 0, r);
        System.arraycopy(elements, 0, a, r, p);
        elements = a;
        head = 0;
        tail = n;
        mask = elements.length - 1;
    }

    /**
     * @param elem adds the given element to this buffer
     */
    public void add(T elem) {
        modcount++;
        elements[tail] = elem;
        tail = (tail + 1) & mask;

        if (head == tail)
            doubleCapacity();
    }

    /**
     * @param index the index of the element to retrieve
     * @return the element stored at the given index
     */
    @SuppressWarnings("unchecked")
    public T get(int index) {
        if (index < 0 || index >= size())
            throw new NoSuchElementException("Illegal index: " + index);

        return (T) elements[(head + index) & mask];
    }

    /**
     * Retrieves an iterator returning all elements in the specified range.
     * @param fromIdx the lower bound of the range (inclusive)
     * @param toIdx the upper bound of the range (exclusive)
     * @return An iterator returning all elements in the specified range
     */
    public Iterator<T> iterator(int fromIdx, int toIdx) {
        return new RBIterator(fromIdx, toIdx);
    }

    /**
     * Retrieves an iterator returning all elements in this buffer
     * @return An iterator returning all elements in this buffer
     */
    public Iterator<T> iterator() {
        return new RBIterator(0, size());
    }

    /**
     * Removes all elements up to the given index (exclusive)
     * @param endIdx the index of the first element to keep.
     */
    public void removeAllUntil(int endIdx) {
        head = (head + endIdx) & mask;
    }

    /**
     * Retrieves the first element of this buffer
     * @return the first element of this buffer
     */
    @SuppressWarnings("unchecked")
    public T peekFirst() {
        if (isEmpty())
            throw new NoSuchElementException("Buffer empty");
        return (T) elements[head];
    }

    /**
     * Retrieves and removes the first element of this buffer
     * @return the first element of this buffer
     */
    @SuppressWarnings("unchecked")
    public T pollFirst() {
        if (isEmpty())
            throw new NoSuchElementException("Buffer empty");
        modcount++;
        T result = (T) elements[head];
        elements[head] = null; // Must null out slot
        head = (head + 1) & mask;
        return result;
    }

    /**
     * Retrieves the last element of this buffer
     * @return the last element of this buffer
     */
    @SuppressWarnings("unchecked")
    public T peekLast() {
        if (isEmpty())
            throw new NoSuchElementException("Buffer empty");
        int t = (tail - 1) & mask;
        return (T) elements[t];
    }

    /**
     * Retrieves and removes the last element of this buffer
     * @return the last element of this buffer
     */
    @SuppressWarnings("unchecked")
    public T pollLast() {
        if (isEmpty())
            throw new NoSuchElementException("Buffer empty");
        modcount++;
        int t = (tail - 1) & mask;
        T result = (T) elements[t];
        elements[t] = null;
        tail = t;
        return result;
    }

    /**
     * @return <code>true</code> if this buffer contains any elements, <code>false</code> otherwise
     */
    public boolean isEmpty() {
        return head == tail;
    }

    /**
     * @return the number of elements stored in this buffer.
     */
    public int size() {
        return (tail - head) & mask;
    }

    /**
     * Iterator implementation for the {@link RingBuffer}. This iterator is fail-fast.
     *
     */
    private class RBIterator implements Iterator<T> {

        /** The end index */
        private final int idxEnd;
        /** The mod count this iterator was created with */
        private final int localMc;
        /** The current index */
        private int idxCurrent;

        /**
         * @param idx_start the index to start at (inclusive)
         * @param idx_end the index to end at (exclusive)
         */
        public RBIterator(int idx_start, int idx_end) {
            this.localMc = modcount;
            this.idxCurrent = (head + idx_start) & mask;
            this.idxEnd = (idxCurrent + (idx_end - idx_start)) & mask;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean hasNext() {
            return idxCurrent != idxEnd;
        }

        /**
         * {@inheritDoc}
         */
        @SuppressWarnings("unchecked")
        @Override
        public T next() {
            if (idxEnd == idxCurrent)
                throw new NoSuchElementException("Cannot iterate beyond range!");
            if (localMc != modcount)
                throw new ConcurrentModificationException("RingBuffer was modified during iteration!");
            int idx = idxCurrent;
            idxCurrent = (idxCurrent + 1) & mask;
            return (T) elements[idx];
        }
    }

}
