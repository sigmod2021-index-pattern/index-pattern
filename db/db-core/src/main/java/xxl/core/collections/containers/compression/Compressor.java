package xxl.core.collections.containers.compression;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * This interface abstracts a byte-oriented data compressor.
 *
 */
public interface Compressor {

	/**
	 * Compresses the given data and returns the compressed result.
	 *
	 * @param b
	 *            the uncompressed input data
	 * @return the compressed result
	 */
	public byte[] compress(byte[] b);

	/**
	 * Uncompresses the given data and returns the uncompressed result.
	 *
	 * @param b
	 *            the compressed data
	 * @param length
	 *            the original (uncompressed) size of the data in bytes
	 * @return the uncompressed data
	 */
	public byte[] decompress(byte[] b, int length);
	
	void writeParams( DataOutput out ) throws IOException;
	
	void restoreParams( DataInput in ) throws IOException;

}
