package xxl.core.cursor;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import sigmod2021.event.Attribute;
import sigmod2021.event.EventSchema;
import sigmod2021.event.SchemaException;
import xxl.core.cursors.AbstractCursor;

/**
 * Cursor for CSV event sources.
 *
 */
public class CSVCursor extends AbstractCursor<Object[]>{
    /** Scanner for inputs */
    private Scanner scanner;
    /** The current line scanned */
    private String line;
    /** Flag indicating of the input has been finished */
    private boolean finished;
    /** The schema of the input data */
    private EventSchema schema;
    /** An optional permutation of the input columns */
    private int[] permutation;
    /** The separator of the columns */
    private String separatorRegex;
    /** The header (if existing) */
    private Map<String, Integer> header;
    /** Flag indicating if the file has a header */
    private boolean useHeader;
    /** Flag indicating whether an entry is skipped on error */
    private boolean skipOnError;

    /**
     * Creates a new CSV cursor.
     *
     * @param fileName the name of the input file
     * @param separatorRegex the separator of the CSV columns
     * @param schema the schema of the data (required for typing)
     * @param header the mapping attribute name --> index
     * @param charsetName the name of the charset
     */
    public CSVCursor(String fileName, String separatorRegex, EventSchema schema, Map<String, Integer> header, String charsetName) {
        this(fileName, separatorRegex, schema, false, charsetName);
        this.header = header;
        this.useHeader = true;
    }

    /**
     * Creates a new CSV cursor.
     *
     * @param fileName the name of the input file
     * @param separatorRegex the separator of the CSV columns
     * @param schema the schema of the data (required for typing)
     * @param header flag indicating if the file contains a CSV header
     * @param charsetName the name of the charset
     */
    public CSVCursor(String fileName, String separatorRegex, EventSchema schema, boolean header, String charsetName) {
        this(fileName, separatorRegex, schema, header, false, charsetName, true);
    }

    /**
     * Creates a new CSV cursor.
     *
     * @param fileName the name of the input file
     * @param separatorRegex the separator of the CSV columns
     * @param schema the schema of the data (required for typing)
     * @param header flag indicating if the file contains a CSV header
     * @param ignoreHeader flag indicating whether the header should be ignored
     * @param charsetName the name of the charset
     * @param skipOnError flag indicating if erroneous entries should be skipped
     */
    public CSVCursor(String fileName, String separatorRegex, EventSchema schema, boolean header, boolean ignoreHeader, String charsetName, boolean skipOnError) {
        try 
        {
        	this.separatorRegex = separatorRegex;
            this.schema = schema;
            this.permutation = new int[schema.getNumAttributes()];
            for(int i = 0; i < permutation.length; i++) {
                permutation[i] = i;
            }
            this.scanner = new Scanner(new File(fileName), charsetName);
            this.header = new HashMap<>();
            this.useHeader = header&&!ignoreHeader;
            this.skipOnError = skipOnError;
            if (header) {
                if (ignoreHeader)
                    scanner.nextLine();
                else
            	    parseHeader();
            } else {
                for(int i = 0 ; i < schema.getNumAttributes(); i++)
                    this.header.put(schema.getAttribute(i).getName().toUpperCase(),i);
            }
            	
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    /**
     * Creates a CSV cursor with permutation.
     *
     * @param fileName the name of the input file
     * @param schema the schema of the data (required for typing)
     * @param permutation the permutation for the output data
     * @param header flag indicating if the file contains a CSV header
     */
    public CSVCursor(String fileName, EventSchema schema, int[] permutation, boolean header) {
        this(fileName,schema,"\t",permutation,header, "UTF-8");
    }

    /**
     * Creates a CSV cursor with permutation.
     *
     * @param fileName the name of the input file
     * @param schema the schema of the data (required for typing)
     * @param separatorRegex the separator of the CSV columns
     * @param permutation the permutation for the output data
     * @param header flag indicating if the file contains a CSV header
     * @param charsetName the name of the charset
     */
    public CSVCursor(String fileName, EventSchema schema, String separatorRegex, int[] permutation, boolean header, String charsetName) {
        this(fileName, separatorRegex, schema, header, charsetName);
        this.permutation = permutation;
    }

    /**
     * Returns the header of the file.
     *
     * @return the header as CSV string
     */
    public String getHeader() {
        EventSchema inter = getPermutedSchema();
        String res = "";
        for(int i = 0; i < inter.getNumAttributes(); i++) {
        	if (i > 0)
                res += separatorRegex;
            res += inter.getAttribute(i).getName();
        }
        return res;
    }

    /**
     * Parses the header of the file.
     */
    private void parseHeader() {
    	String l = scanner.nextLine();
    	String[] a = l.split(separatorRegex);
    	for(int i = 0; i < a.length; i++) {
    		header.put(a[i].toUpperCase(), i);
    	}
    }

    @Override
    public void close() {
        super.close();
        scanner.close();
    }

    @Override
    protected boolean hasNextObject() {
        if (finished)
            return false;
        if (line == null && scanner.hasNextLine())
            line = scanner.nextLine();
        if (line == null) {
            finished = true;
            return false;
        } else
            return true;
    }

    @Override
    protected Object[] nextObject() {
        if (line == null && !finished && scanner.hasNextLine())
            line = scanner.nextLine();
        if (line == null) {
            finished = true;
            return null;
        }
        String[] values = line.split(separatorRegex);
        if (scanner.hasNextLine())
            line = scanner.nextLine();
        else
            finished = true;
        if (values.length < header.size()) {
            // Pad to size
            List<String> l = new ArrayList<>(Arrays.asList(values));
            for (int i = 0; i < header.size()-values.length; i++)
                l.add("");
            values = l.toArray(values);
        }
        Object[] result = new Object[schema.getNumAttributes()];
        try{
            for(int i = 0; i < result.length; i++) {
            	final Attribute attr = schema.getAttribute(i);
                switch (attr.getType()) {
                    case BYTE: result[permutation[i]] = Byte.parseByte(values[getAttributeIndex(attr,i)]); break;
                    case SHORT: result[permutation[i]] = Short.parseShort(values[getAttributeIndex(attr,i)]); break;
                    case INTEGER: result[permutation[i]] = Integer.parseInt(values[getAttributeIndex(attr,i)]); break;
                    case DOUBLE: result[permutation[i]] = Double.parseDouble(values[getAttributeIndex(attr,i)]); break;
                    case FLOAT: result[permutation[i]] = Float.parseFloat(values[getAttributeIndex(attr,i)]); break;
                    case LONG: result[permutation[i]] = Long.parseLong(values[getAttributeIndex(attr,i)]); break;
                    default: result[permutation[i]] = values[getAttributeIndex(attr,i)]; //String
                }
            }
        } catch(Exception nfe) {
            if (!skipOnError)
                throw nfe;
        }
        return result;
    }

    /**
     * Returns the index of the given attribute.
     *
     * @param a the attribute
     * @param index the expected index
     * @return the index of the requested attribute
     */
    private int getAttributeIndex(Attribute a, int index) {
    	if (!useHeader)
    		return index;
    	else
    		return header.get(a.getName().toUpperCase());
    }

    /**
     * Returns the permuted schema
     *
     * @return the permuted schema
     */
    public EventSchema getPermutedSchema() {
        Attribute[] permuted = new Attribute[schema.getNumAttributes()];
        for(int i = 0; i < permuted.length; i++)
            permuted[permutation[i]] = schema.getAttribute(i);

        try {
			return new EventSchema(permuted);
		}
		catch (SchemaException e) {
			throw new RuntimeException(e);
		}
    }
}
