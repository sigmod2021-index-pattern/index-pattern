package xxl.core.indexStructures;

import java.util.function.Supplier;

/**
 * This interface guarantees the index structure to be DOT plottable.
 */
public interface Plottable {

	/**
	 * Escape the text for the usage in DOT.
	 * 
	 * @param text
	 * @return The escaped string.
	 */
	static String escapeText(String text) {
		return text.replaceAll("([\\[\\]\\(\\)\\-\\>\\<\\{\\}])", "\\\\$1");
	}

	/**
	 * Creates unique names using a specified prefix.
	 * 
	 * @param prefix
	 *            name prefix
	 * @return A name generator via {@link Supplier}.
	 */
	static Supplier<String> getNameGenerator(String prefix) {
		return new Supplier<String>() {

			int nodeNumber = 0;

			@Override
			public String get() {
				return prefix + "node" + nodeNumber++;
			}
		};
	}

	/**
	 * Creates unique names.
	 *
	 * @param prefix
	 *            name prefix
	 * @return A name generator via {@link Supplier}.
	 */
	static Supplier<String> getNameGenerator() {
		return getNameGenerator("");
	}

	/**
	 * Retrieves the plot as a string.
	 * 
	 * @return plot code.
	 */
	public String getPlot();

	/**
	 * Appends the plot to an existing {@link StringBuilder}.
	 * 
	 * @param buffer
	 *            The buffer should be used to append the plot.
	 * @param prefix
	 *            Every node must use this prefix.
	 */
	public void appendPlot(StringBuilder buffer, String prefix);
}
