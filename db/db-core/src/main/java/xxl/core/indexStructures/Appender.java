package xxl.core.indexStructures;

import java.util.concurrent.Future;

import sigmod2021.db.event.Persistent;
import sigmod2021.db.event.TID;
import sigmod2021.db.queries.NoSuchEventException;
import xxl.core.cursors.Cursor;
import xxl.core.indexStructures.BPlusLink.IndexEntry;
import xxl.core.indexStructures.BPlusLink.Node;
import xxl.core.util.Pair;

/**
 * Abstraction of an appender used to append data to a B+-Tree.
 *
 */
public interface Appender<TIN,TOUT extends Persistent<TIN>> {

	/**
	 * Flushes the complete path to the container and the container to disk.
	 * After a call of this method, the appender is closed and should not be
	 * used anymore.
	 */
	void flushPathAndClose();

	/**
	 * Flushes the complete path to the container (not necessarily the
	 * container).
	 */
	void flushPath();

	/**
	 * Inserts a new entry into the BPlusTree and returns the position of the
	 * element in the underlying container.
	 *
	 * @param entry
	 *            new entry to insert into the BPlusTree
	 * @return the position (block number, position in block) of the inserted
	 *         element in the underlying container
	 */
	Future<TID> insertEntry(TIN entry);

	/**
	 * Executes an out-of-order insert on the tree.
	 *
	 * @param entry
	 *            the entry to be inserted
	 * @return the event ID of the inserted element
	 */
	Future<TID> outOfOrderInsert(TIN entry);

	/**
	 * Returns, if data has been inserted into the appender after creation.
	 *
	 * @return True, if data has been inserted after creation, false otherwise
	 */
	boolean hasData();

	/**
	 * Recovers the tree´s right flank. The appender has to be loaded again
	 * afterwards!
	 *
	 */
	void recover();

	/**
	 * Returns a temporary root for the underlying tree. This does not
	 * necessarily insert the root into the tree.
	 *
	 * @param updateAggregates
	 *            indicates whether aggregates should be updated
	 * @return a pair consisting of a temporary root node an its index entry
	 */
	Pair<IndexEntry, Node> getTempRoot(boolean updateAggregates);

	/**
	 * Queries the given range of the underlying tree and returns the results.
	 *
	 * @param minKey
	 *            the minimum value of the query range
	 * @param maxKey
	 *            the maximum value of the query range
	 * @return a cursor delivering the query results
	 */
	Cursor<TOUT> query(Long minKey, Long maxKey);
	
	Cursor<TOUT> find(TID id) throws NoSuchEventException;
	
	int getMaxLeafEntries();
	
	int getMaxIndexEntries();
}
