package xxl.core.cursor;

import java.util.Arrays;
import java.util.NoSuchElementException;


/**
 *
 */
public abstract class AbstractDoubleCursor<T> implements DoubleCursor<T> {
	
	private static enum State { INITIALIZED, OPEN, VALID, CLOSED };
	
	private State state = State.INITIALIZED;
	
	private Direction prev = new Direction();
	private Direction next = new Direction();
	
	private Peek peekPrev = new Peek();
	private Peek peekNext = new Peek();
	
	private void ensureState(State...validStates) {
		for ( State s : validStates )
			if ( s == this.state )
				return;
		
		throw new IllegalArgumentException(String.format(
				"Illegal state (%s) for desired action. Required: %s", 
				state, Arrays.toString(validStates), validStates));
	}
	
	protected abstract void doOpen();
	
	protected abstract void doClose();
	
	protected abstract boolean computeHasNext();
	
	protected abstract T computeNext();
	
	protected abstract boolean computeHasPrevious();
	
	protected abstract T computePrevious();
	
	protected void doUpdate(T item) {
		throw new UnsupportedOperationException("Updates not supported by this cursor");
	}
	
	protected void doReset() {
		throw new UnsupportedOperationException("Reset not supported by this cursor");
	}
	
	protected void doRemove() {
		throw new UnsupportedOperationException("Remove not supported by this cursor");
	}
	
	protected T computePeek() {
		throw new UnsupportedOperationException("Peek not supported by this cursor");
	}
	
	protected T computePeekBack() {
		throw new UnsupportedOperationException("PeekBack not supported by this cursor");
	}
	

	/**
	 * @{inheritDoc}
	 */
	@Override
	public final void open() {
		ensureState(State.INITIALIZED);
		doOpen();
		this.state = State.OPEN;
	}

	/**
	 * @{inheritDoc}
	 */
	@Override
	public final void close() {
		ensureState(State.OPEN, State.VALID);
		doClose();
		this.state = State.CLOSED;
	}
	
	/**
	 * @{inheritDoc}
	 */
	@Override
	public final boolean hasNext() throws IllegalStateException {
		ensureState(State.OPEN, State.VALID);
		if ( !next.computedHasMore )
			next.setHasMore(computeHasNext());
		return next.hasMore;
	}

	/**
	 * @{inheritDoc}
	 */
	@Override
	public final T next() throws IllegalStateException, NoSuchElementException {
		ensureState(State.OPEN, State.VALID);
		state = State.VALID;
		prev.wasValid = false;
		
		if ( !next.computedHasMore ) {
			next.setHasMore(computeHasNext());
		}
		if ( !next.hasMore )
			throw new NoSuchElementException("No next element!");
		
		try {
			// Save peekLast
			if ( next.wasValid )
				peekPrev.setElem(next.elem);
			else
				peekPrev.reset();
			
			next.setElem(computeNext());
			return next.elem;
		} finally {
			peekNext.reset();
			prev.reset();
			next.reset();			
		}
	}
	
	/**
	 * @{inheritDoc}
	 */
	@Override
	public final boolean hasPrevious() throws IllegalStateException, NoSuchElementException {
		ensureState(State.OPEN, State.VALID);
		if ( !prev.computedHasMore )
			prev.setHasMore(computeHasPrevious());
		return prev.hasMore;
	}
	
	/**
	 * @{inheritDoc}
	 */
	@Override
	public final T previous() throws IllegalStateException, NoSuchElementException {
		ensureState(State.OPEN, State.VALID);
		state = State.VALID;
		next.wasValid = false;
		
		if ( !prev.computedHasMore ) {
			prev.setHasMore(computeHasPrevious());
		}
		if ( !prev.hasMore )
			throw new NoSuchElementException("No next element!");
		
		try {
			// Save peekLast
			if ( prev.wasValid )
				peekNext.setElem(prev.elem);
			else
				peekNext.reset();
			
			prev.setElem(computePrevious());
			return prev.elem;
		} finally {
			peekPrev.reset();
			prev.reset();
			next.reset();			
		}
	}
	
	/**
	 * @{inheritDoc}
	 */
	@Override
	public final T peek() throws IllegalStateException, NoSuchElementException, UnsupportedOperationException {
		if ( !supportsPeek() )
			AbstractDoubleCursor.this.computePeek();
		ensureState(State.OPEN, State.VALID);
		if ( !peekNext.computedElem )
			peekNext.setElem(computePeek());
		
		return peekNext.elem;
	}
	
	/**
	 * @{inheritDoc}
	 */
	@Override
	public final T peekBack() throws IllegalStateException, NoSuchElementException, UnsupportedOperationException {
		if ( !supportsPeekBack() )
			AbstractDoubleCursor.this.computePeekBack();
		ensureState(State.OPEN, State.VALID);
		if ( !peekPrev.computedElem )
			peekPrev.setElem(computePeekBack());
		
		return peekPrev.elem;
	}
	
	/**
	 * @{inheritDoc}
	 */
	@Override
	public final void reset() throws UnsupportedOperationException {
		if ( !supportsReset() )
			AbstractDoubleCursor.this.reset();
		ensureState(State.OPEN,State.VALID);
		doReset();
		resetAll();
	}
	
	/**
	 * @{inheritDoc}
	 */
	@Override
	public final void remove() throws IllegalStateException, UnsupportedOperationException {
		if ( !supportsRemove() )
			AbstractDoubleCursor.this.remove();
		ensureState(State.VALID);
		doRemove();
		resetAll();
	}
	
	/**
	 * @{inheritDoc}
	 */
	@Override
	public final void update(T object) throws IllegalStateException, UnsupportedOperationException {
		if ( !supportsUpdate() )
			AbstractDoubleCursor.this.update(object);
		ensureState(State.VALID);
		doUpdate(object);
		resetAll();
	}
	
	/**
	 * @{inheritDoc}
	 */
	@Override
	public boolean supportsPeek() {
		return false;
	}

	/**
	 * @{inheritDoc}
	 */
	@Override
	public boolean supportsPeekBack() {
		return false;
	}

	/**
	 * @{inheritDoc}
	 */
	@Override
	public boolean supportsRemove() {
		return false;
	}

	/**
	 * @{inheritDoc}
	 */
	@Override
	public boolean supportsUpdate() {
		return false;
	}

	/**
	 * @{inheritDoc}
	 */
	@Override
	public boolean supportsReset() {
		return false;
	}
	
	private void resetAll() {
		prev.reset();
		next.reset();
		peekPrev.reset();
		peekNext.reset();
	}
	
	protected class Peek {
		protected boolean computedElem = false;
		protected T elem = null;
		protected boolean wasValid = false;
		
		protected void reset() {
			computedElem = false;
		}
		
		/**
		 * @param elem the elem to set
		 */
		protected void setElem(T elem) {
			this.wasValid = true;
			this.computedElem = true;
			this.elem = elem;
		}
	}
	
	protected class Direction extends Peek {
		protected boolean computedHasMore = false;
		protected boolean hasMore = false;
		
		protected void reset() {
			super.reset();
			computedHasMore = false;
			hasMore = false;
		}
		
		/**
		 * @param hasMore the hasMore to set
		 */
		protected void setHasMore(boolean hasMore) {
			this.computedHasMore = true;
			this.hasMore = hasMore;
		}
	}

}
