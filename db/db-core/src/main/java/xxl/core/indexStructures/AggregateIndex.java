package xxl.core.indexStructures;

/**
 * Represents index structures with aggregate query support.
 *
 */
public interface AggregateIndex<N extends BPlusTree.Node> {

	/**
	 * Represents an aggregate.
	 */
	public interface Aggregation {

		/**
		 * Updates the aggregate with the given one.
		 *
		 * @param agg
		 *            the aggregate to be included
		 */
		void update(Aggregation agg);

		/**
		 * Returns the numbers.
		 *
		 * @return the numbers
		 */
		Number[] getValues();
	}

	/**
	 * Returns the aggregates of the index.
	 *
	 * @param attribute
	 *            the attribute to aggregate
	 * @param minKey
	 *            the minimum key
	 * @param maxKey
	 *            the maximum key
	 * @return all aggregates of the given attribute within the given key range
	 */
	Aggregation getAggregate(String attribute, Comparable<?> minKey, Comparable<?> maxKey, Comparable<?> globalMinKey);

	/**
	 * Returns the aggregates of the index.
	 *
	 * @param attribute
	 *            the attribute to aggregate
	 * @param minKey
	 *            the minimum key
	 * @param maxKey
	 *            the maximum key
	 * @param nodeLeft
	 * 				
	 * @return all aggregates of the given attribute within the given key range
	 */
	Aggregation getAggregate(String attribute, Comparable<?> minKey, Comparable<?> maxKey, N node, Long nodeLeft);
}
