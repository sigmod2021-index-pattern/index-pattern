package xxl.core.indexStructures.appender;

import java.util.NoSuchElementException;

import sigmod2021.db.event.Persistent;
import sigmod2021.db.event.TID;
import xxl.core.collections.containers.Container;
import xxl.core.cursor.AbstractDoubleCursor;
import xxl.core.functions.Functional;
import xxl.core.indexStructures.BPlusLink;
import xxl.core.indexStructures.BPlusLink.Node;
import xxl.core.indexStructures.FastAggregatedBPlusTree.IndexEntry;


/**
 *
 */
public class LeafLevelCursor<T, TOUT extends Persistent<T>> extends AbstractDoubleCursor<TOUT> {
	
	/**
	 * The level of a leaf node.
	 */
	protected static final int	LEAF_LEVEL	= 0;
	
	private static enum Direction {FORWARD,BACKWARD};
	
	private final Functional.BinaryFunction<TID, T, TOUT> generateOut;
	
	private final Container container;
	
	private final TID begin;
	
	private Direction direction = Direction.FORWARD;
	
	private BPlusLink.Node currentNode = null;
	
	private long currentNodeId = -1;
	
	private int offset = -1;

	/**
	 * Creates a new LeafLevelCursor instance
	 * @param container
	 */
	public LeafLevelCursor(Container container, TID begin, Functional.BinaryFunction<TID, T, TOUT> generateOut) {
		this.container = container;
		this.begin = begin;
		this.generateOut = generateOut;
	}
	
	/**
	 * @{inheritDoc}
	 */
	@Override
	protected void doOpen() {
		doReset();
	}

	/**
	 * @{inheritDoc}
	 */
	@Override
	protected void doClose() {
		offset = -1;
		currentNodeId = -1;
		currentNode = null;
	}
	
	/**
	 * @{inheritDoc}
	 */
	@Override
	public boolean computeHasPrevious() {
		if ( direction == Direction.BACKWARD )
			return currentNode != null && (offset >= 0 || currentNode.previousNeighbor() != null);
		else
			return currentNode != null && (offset > 0 || currentNode.previousNeighbor() != null);
	}

	/**
	 * @{inheritDoc}
	 */
	@SuppressWarnings("unchecked")
	@Override
	public TOUT computePrevious() {
		setDirection(Direction.BACKWARD);
		
		// Change over if required
		if ( currentNode != null && offset < 0 && currentNode.previousNeighbor() != null ) {
			changeOver((IndexEntry) currentNode.previousNeighbor(), offset);
		}
		if ( currentNode != null && offset >= 0 ) {
			TID id = new TID(currentNodeId, offset--);
			return generateOut.invoke(id, (T) currentNode.getEntry(id.getOffset()));
		}
		else {
			throw new NoSuchElementException("Cannot go beyond last entry!");
		}
	}


	/**
	 * @{inheritDoc}
	 */
	@Override
	protected boolean computeHasNext() {
		if ( direction == Direction.FORWARD )
			return currentNode != null && (offset < currentNode.number() || currentNode.nextNeighbor() != null);
		else
			return currentNode != null && (offset < currentNode.number() -1 || currentNode.nextNeighbor() != null);
	}

	/**
	 * @{inheritDoc}
	 */
	@SuppressWarnings("unchecked")
	@Override
	protected TOUT computeNext() {
		setDirection(Direction.FORWARD);
		
		// Change over if required
		if ( currentNode != null && offset >= currentNode.number() && currentNode.nextNeighbor() != null ) {
			int delta = offset + 1 - currentNode.number();
			changeOver((IndexEntry) currentNode.nextNeighbor(), delta);
		}
		if ( currentNode != null && offset < currentNode.number() ) {
			TID id = new TID(currentNodeId, offset++);
			return generateOut.invoke(id, (T) currentNode.getEntry(id.getOffset()));
		}
		else {
			throw new NoSuchElementException("Cannot go beyond last entry!");
		}
	}
	
	private void changeOver(IndexEntry ie, int delta) {
		currentNode = ie.get();
		currentNodeId = (Long) ie.id();
		offset = delta < 0 ? currentNode.number() + delta : delta - 1;
	}
	
	/**
	 * @param desired the direction to set
	 */
	private void setDirection(Direction desired) {
		if ( desired == this.direction )
			return;
		
		// offset points to previous, let it point to next
		if ( this.direction == Direction.BACKWARD ) {
			this.direction = Direction.FORWARD;
			offset+=2;
		}
		// offset points to next, let it point to previous
		else {
			this.direction = Direction.BACKWARD;
			offset-=2;
		}
	}

	/**
	 * @{inheritDoc}
	 */
	@Override
	protected void doReset() throws UnsupportedOperationException {
		this.currentNodeId = begin.getBlockId();
		this.currentNode = (Node) container.get(begin.getBlockId());
		
		if ( currentNode.level > LEAF_LEVEL )
			throw new NoSuchElementException("Given page is a non-leaf level page.");
		else if ( begin.getOffset() < 0 || begin.getOffset() > currentNode.number() )
			throw new NoSuchElementException(
				"Page " + begin.getBlockId() + " does not contain an entry with offset " + begin.getOffset());
		
		this.offset = begin.getOffset();
		this.direction = Direction.FORWARD;
	}

	/**
	 * @{inheritDoc}
	 */
	@Override
	public boolean supportsReset() {
		return true;
	}
}
