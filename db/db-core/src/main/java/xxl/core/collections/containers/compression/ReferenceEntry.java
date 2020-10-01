package xxl.core.collections.containers.compression;

/**
 * Defines a reference entry for an updated block.
 *
 */
public class ReferenceEntry {

	/**
	 * Stores the previous written id. This is required to calculate the
	 * position of the macro block within the container.
	 */
	private final long	previousWrittenId;
	/**
	 * Stores the physical address for the reference
	 */
	private final long	physicalAddress;

	/**
	 * Creates a new reference entry.
	 *
	 * @param physicalAddress
	 *            the physical address
	 * @param previousWrittenId
	 *            the id of the previous written block
	 */
	public ReferenceEntry(final long physicalAddress, final long previousWrittenId) {
		this.physicalAddress = physicalAddress;
		this.previousWrittenId = previousWrittenId;
	}

	/**
	 * Returns the id of the previous written block.
	 *
	 * @return the address of the previous written block
	 */
	public long getPreviousWrittenId() {
		return this.previousWrittenId;
	}

	/**
	 * Returns the physical address (macro block + entry number).
	 *
	 * @return the physical address for the new position
	 */
	public long getPhysicalAddress() {
		return this.physicalAddress;
	}
}
