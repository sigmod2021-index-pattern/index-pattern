package xxl.core.collections.containers.compression;

import java.io.DataInput;
import java.io.DataOutput;

/**
 * This compressor implementation does nothing.
 * 
 * @see Compressor
 */
public class NoCompressor implements Compressor{

	@Override
	public byte[] compress(byte[] b) {
		return b;
	}

	@Override
	public byte[] decompress(byte[] b, int length) {
		return b;
	}

	/**
	 * @{inheritDoc}
	 */
	@Override
	public void writeParams(DataOutput out) {
	}

	/**
	 * @{inheritDoc}
	 */
	@Override
	public void restoreParams(DataInput in) {
	}

}
