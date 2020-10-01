package xxl.core.collections.containers.compression;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import xxl.core.io.UnsafeDataInput;
import xxl.core.io.raw.RawAccess;

/**
 * A Recovery manager for not written TLB entries.
 *
 */
public class TLBRecovery {

	/** The container file */
	private final RawAccess	raf;
	/** The size of a physical block (i.e. TLB entry) */
	private final int		blockSize;
	/** The size of a preamble */
	private final long		preambleSize;
	/** The position of the last TLB entry found during recovery. */
	private long			lastTLBPosition		= -1;
	/** The last written physical address */
	private long			lastPhysicalAddress	= -1;

	/**
	 * Creates a new TLB recovery component for the given container file.
	 *
	 * @param raf
	 *            the container file containing the TLB entries
	 * @param blockSize
	 *            the size of a block on disk
	 */
	public TLBRecovery(final RawAccess raf, final int blockSize, final long preambleSize) {
		this.raf = raf;
		this.blockSize = blockSize;
		this.preambleSize = preambleSize;
	}

	/**
	 * Recovers the TLB (if possible).
	 *
	 * @return the recovered TLB or a new one, if recovery was not successful
	 * @throws IOException
	 */
	public TLB recover() throws IOException {

		// 1. Find last fully written block
		long position = (this.raf.getNumSectors() - 1) * this.blockSize;

		// Try to interpret as TLB entry
		TLB.TLBEntry entry = read(position / this.blockSize);
		while (entry == null && position >= (this.blockSize + this.preambleSize)) {
			// Find position of previous entry
			position -= this.blockSize;
			entry = read(position / this.blockSize);
		}

		if ( entry == null )
			return new TLB(this.raf, this.blockSize);
		this.lastTLBPosition = position;
		// continue restoring the TLB entries
		final List<TLB.TLBEntry> entries = recover(entry, position);

		// Create TLB structure (add entries to top)
		final TLB tlb = new TLB(this.raf, this.blockSize);
		tlb.readBuffer.clear();
		tlb.writeBuffer.clear();

		// Create new entries for already written levels
		final int lowerLevelCount = entry.level + 1;
		for (int i = lowerLevelCount - 1; i >= 0; i--) {
			final TLB.TLBEntry lowerLevelEntry = new TLB.TLBEntry(getBufferSize());
			lowerLevelEntry.level = (short) i;
			lowerLevelEntry.previousParentEntry = entry.previousParentEntry;
			lowerLevelEntry.previousEntry = position;
			tlb.writeBuffer.add(i, lowerLevelEntry);
			tlb.readBuffer.add(i, lowerLevelEntry);
			if ( i > 0 ) {
				position -= this.blockSize;
				entry = read(position / this.blockSize);
			}
		}

		// invert ordering of addresses within the TLB entries and add to tlb
		for (final TLB.TLBEntry tlbEntry : entries) {
			Collections.reverse(tlbEntry.entries);
			tlb.writeBuffer.add(tlbEntry.level, tlbEntry);
			tlb.readBuffer.add(tlbEntry.level, tlbEntry);
		}

		// Set mapIDs of entries
		for (int i = tlb.writeBuffer.size() - 2; i >= 0; i--) {
			final TLB.TLBEntry parentEntry = tlb.writeBuffer.get(i + 1);
			tlb.writeBuffer.get(i).mapID = (long) (parentEntry.mapID * Math.pow(getBufferSize(), i + 1L)
					+ parentEntry.entries.size());
		}

		// Set the last written physical address
		if ( tlb.writeBuffer.get(0).previousEntry != -1 ) {
			final TLB.TLBEntry prev = read(tlb.writeBuffer.get(0).previousEntry / this.blockSize);
			this.lastPhysicalAddress = prev.entries.get(prev.entries.size() - 1);
		}

		return tlb;
	}

	/**
	 * Returns the last found TLB entry position.
	 *
	 * @return the physical position of the last TLB entry found in the
	 *         container.
	 */
	public long getLastTLBPosition() {
		return this.lastTLBPosition;
	}

	/**
	 * Returns the last written physical address.
	 *
	 * @return the last written physical address
	 */
	public long getLastPhysicalAddress() {
		return this.lastPhysicalAddress;
	}

	/**
	 * Calculates the maximum number of entries contained in a TLB entry.
	 *
	 * @return the maximum number of entries per TLB
	 */
	private int getBufferSize() {
		return (this.blockSize - 32) / 8;
	}

	/**
	 * Tries to read a TLB entry. If reading was not successful, null is
	 * returned.
	 *
	 * @return the TLB entry of null, if no TLB entry was found
	 * @throws IOException
	 */
	private TLB.TLBEntry read(final long blockNumber) throws IOException {
		// Try to interpret as TLB entry
		final byte[] b = new byte[this.blockSize];
		this.raf.read(b, blockNumber);
		if ( isTLBEntry(b) )
			return deserialize(b);
		else
			return null;
	}

	/**
	 * Iterates all previous entries of the given one, which belong to the next
	 * parent entry, and inserts them into the next parent entry.
	 *
	 * @param entry
	 *            the entry
	 * @param parentEntry
	 *            the parent entry
	 * @throws IOException
	 */
	private void iterateTLBLevel(final TLB.TLBEntry entry, final TLB.TLBEntry parentEntry) throws IOException {
		TLB.TLBEntry cursor = entry;
		// Iterate current level until start
		while (cursor != null && cursor.previousEntry != -1
				&& cursor.previousParentEntry == entry.previousParentEntry) { // Discover all previous entries
			final TLB.TLBEntry oldCursor = cursor;
			cursor = read(cursor.previousEntry / this.blockSize);
			if ( cursor.previousParentEntry == entry.previousParentEntry ) {
				parentEntry.entries.add(oldCursor.previousEntry);
			}
		}
	}

	/**
	 * Recovers all non-written entries (the current write buffer) of the TLB).
	 *
	 * @param entry
	 *            the last written TLB entry
	 * @param position
	 *            the position of the last written TLB entry in the container
	 * @return a list containing all non-written TLB entries
	 * @throws IOException
	 */
	private List<TLB.TLBEntry> recover(final TLB.TLBEntry entry, final long position) throws IOException {
		final int bufferSize = getBufferSize();
		final List<TLB.TLBEntry> entries = new ArrayList<>();

		TLB.TLBEntry parentEntry = new TLB.TLBEntry(bufferSize);
		parentEntry.level = (short) (entry.level + 1);
		parentEntry.entries.add(position);

		TLB.TLBEntry cursor = entry;

		// Iterate the levels
		while (cursor != null) { // Discover all previous entries
			parentEntry.previousEntry = cursor.previousParentEntry;
			// Iterate level
			iterateTLBLevel(cursor, parentEntry);
			entries.add(parentEntry); // add parent entry to entries
			// Switch to next level
			final TLB.TLBEntry oldParent = parentEntry;
			if ( cursor.previousParentEntry != -1 ) {
				parentEntry = new TLB.TLBEntry(bufferSize);
				parentEntry.level = (short) (cursor.level + 2);
				parentEntry.entries.add(cursor.previousParentEntry);
				cursor = read(cursor.previousParentEntry / this.blockSize);
				oldParent.previousParentEntry = cursor.previousParentEntry;
			}
			else {
				cursor = null;
			}
		}

		return entries;
	}

	/**
	 * Returns if the given byte array is an TLBEntry.
	 *
	 * @param b
	 *            the byte array to check
	 * @return true, if the given byte array is a TLBEntry, false otherwise
	 * @throws IOException
	 */
	private boolean isTLBEntry(final byte[] b) throws IOException {
		final DataInput dataIn = new UnsafeDataInput(new ByteArrayInputStream(b));
		final short tag = dataIn.readShort();
		final int level = dataIn.readShort();
		/*long previousEntry = */dataIn.readLong();
		/*long previousParentEntry =  */dataIn.readLong();
		/*long nextMacroBlockID =  */dataIn.readLong();
		final int count = dataIn.readInt();

		return level >= 0 && count == getBufferSize() && tag == TLB.TLB_TAG;
	}

	/**
	 * Deserializes a TLB entry.
	 *
	 * @param b
	 *            the binary representation of the TLBEntry
	 * @return the deserialized TLBEntry
	 * @throws IOException
	 */
	private TLB.TLBEntry deserialize(final byte[] b) throws IOException {
		final int bufferSize = getBufferSize();
		final TLB.TLBEntry entry = new TLB.TLBEntry(bufferSize);
		final DataInput dataIn = new UnsafeDataInput(new ByteArrayInputStream(b));
		dataIn.readShort(); // tag
		entry.level = dataIn.readShort();
		entry.previousEntry = dataIn.readLong();
		entry.previousParentEntry = dataIn.readLong();
		entry.nextMacroBlockID = dataIn.readLong();
		final int count = dataIn.readInt();
		for (int i = 0; i < count; i++) {
			entry.entries.add(dataIn.readLong());
		}
		return entry;
	}
}
