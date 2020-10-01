package xxl.core.collections.containers.io;

import java.util.Iterator;

import xxl.core.io.converters.Converter;
import xxl.core.io.converters.Converters.SerializationMode;

/**
 * A suspendable converter container.
 *
 */
public class SuspendableConverterContainer extends ConverterContainer implements SuspendableContainer {

	/**
	 * Flag indicating if the stream is currently suspended
	 */
	private boolean suspended;

	/**
	 * Constructs a new ConverterContainer that decorates the specified
	 * container and uses the specified converter for converting its elements.
	 *
	 * @param container
	 *            the underlying container that is used for storing the
	 *            converted elements.
	 * @param converter
	 *            the converter that is used for converting the elements of this
	 *            container.
	 */
	public SuspendableConverterContainer(final SuspendableContainer container, final Converter<?> converter) {
		super(container, converter);
	}

	/**
	 * Constructs a new ConverterContainer that decorates the specified
	 * container and uses the specified converter for converting its elements.
	 *
	 * @param container
	 *            the underlying container that is used for storing the
	 *            converted elements.
	 * @param converter
	 *            the converter that is used for converting the elements of this
	 *            container.
	 */
	public SuspendableConverterContainer(final SuspendableContainer container, final Converter<?> converter,
			final SerializationMode mode, final int buffer) {
		super(container, converter, mode, buffer);
	}

	@Override
	public void suspend() {
		((SuspendableContainer) this.container).suspend();
		this.suspended = true;
	}

	@Override
	public void resume() {
		((SuspendableContainer) this.container).resume();
		this.suspended = false;
	}

	@Override
	public boolean isSuspended() {
		return this.suspended;
	}

	@Override
	public Iterator<?> idsBackwards() {
		return ((SuspendableContainer) this.container).idsBackwards();
	}

	@Override
	public double getCompressionRatio() {
		return ((SuspendableContainer) this.container).getCompressionRatio();
	}

	@Override
	public long getAllocatedSpace() {
		return ((SuspendableContainer) this.container).getAllocatedSpace();
	}

	@Override
	public void allocateSpace(final long space) {
		((SuspendableContainer) this.container).allocateSpace(space);
	}
}
