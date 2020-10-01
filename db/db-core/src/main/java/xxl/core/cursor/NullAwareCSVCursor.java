package xxl.core.cursor;

import sigmod2021.event.Attribute;
import sigmod2021.event.EventSchema;
import sigmod2021.event.SchemaException;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Scanner;


/**
 * Cursor for CSV event sources.
 */
public class NullAwareCSVCursor implements MinimalCursor<Object[]>{
    /** Scanner for inputs */
    private Scanner scanner;
    /** The current line scanned */
    private String line;
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
    /** Flag indicating whether an entry containing a null value anywhere is skipped on error */
    private boolean skipNulls = false;
    /** Flag indicating whether Strings should be trimmed */
    private boolean trimStrings = false;
    /** Flag indicating whether empty values should be skipped */
    private boolean skipEmptyValues = false;

    private Object[] nextValue = null;

    /**
     * Creates a new CSV cursor.
     *
     * @param fileName the name of the input file
     * @param separatorRegex the separator of the CSV columns
     * @param schema the schema of the data (required for typing)
     * @param header the mapping attribute name --> index
     * @param charsetName the name of the charset
     */
    public NullAwareCSVCursor(String fileName, String separatorRegex, EventSchema schema, Map<String, Integer> header, String charsetName) {
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
    public NullAwareCSVCursor(String fileName, String separatorRegex, EventSchema schema, boolean header, String charsetName) {
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
    public NullAwareCSVCursor(String fileName, String separatorRegex, EventSchema schema, boolean header, boolean ignoreHeader, String charsetName, boolean skipOnError) {
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
    public NullAwareCSVCursor(String fileName, EventSchema schema, int[] permutation, boolean header) {
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
    public NullAwareCSVCursor(String fileName, EventSchema schema, String separatorRegex, int[] permutation, boolean header, String charsetName) {
        this(fileName, separatorRegex, schema, header, charsetName);
        this.permutation = permutation;
    }

    public void setNullConfig(boolean skipNulls, boolean skipEmptyValues, boolean trimStrings) {
        this.skipNulls = skipNulls;
        this.skipEmptyValues = skipEmptyValues;
        this.trimStrings = trimStrings;
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
    public void open() {
        if(!skipOnError && skipNulls)
            checkHeader();
        nextValue = nextObject();
    }

    private void checkHeader(){
        for (String s : schema.getAttributeNames()) {
            if(!header.containsKey(s))
                throw new IllegalArgumentException("CSV file does not have the field " + s);
        }
    }

    @Override
    public void close() {
        scanner.close();
    }

    @Override
    public boolean hasNext() throws IllegalStateException {
        return nextValue != null;
    }

    @Override
    public Object[] next() throws IllegalStateException, NoSuchElementException {
        Object[] result = nextValue;
        nextValue = nextObject();
        return result;
    }

    private Object[] nextObject() {
    	Object[] result = null;
    	outer: while ( scanner.hasNextLine() && result == null ) {
            line = scanner.nextLine();
	        String[] values = line.split(separatorRegex);
	        if (values.length < header.size()) {
	            // Pad to size
	            List<String> l = new ArrayList<>(Arrays.asList(values));
	            for (int i = 0; i < header.size()-values.length; i++)
	                l.add("");
	            values = l.toArray(values);
	        }
	        Object[] tmp = new Object[schema.getNumAttributes()];
	        try{
	            for(int i = 0; i < tmp.length; i++) {
	            	final Attribute attr = schema.getAttribute(i);
	                if(skipNulls && values[getAttributeIndex(attr,i)].trim().equalsIgnoreCase("null")){
	                    continue outer;
	                }
	                if( (skipEmptyValues || attr.getType() != Attribute.DataType.STRING) && values[getAttributeIndex(attr,i)].trim().equalsIgnoreCase("")){
	                    continue outer;
	                }
	                switch (attr.getType()) {
	                    case BYTE: tmp[permutation[i]] = Byte.parseByte(values[getAttributeIndex(attr,i)]); break;
	                    case SHORT: tmp[permutation[i]] = Short.parseShort(values[getAttributeIndex(attr,i)]); break;
	                    case INTEGER: tmp[permutation[i]] = Integer.parseInt(values[getAttributeIndex(attr,i)]); break;
	                    case DOUBLE: tmp[permutation[i]] = Double.parseDouble(values[getAttributeIndex(attr,i)]); break;
	                    case FLOAT: tmp[permutation[i]] = Float.parseFloat(values[getAttributeIndex(attr,i)]); break;
	                    case LONG: tmp[permutation[i]] = Long.parseLong(values[getAttributeIndex(attr,i)]); break;
	                    default: tmp[permutation[i]] = trimStrings ? values[getAttributeIndex(attr,i)].trim() : values[getAttributeIndex(attr,i)]; //String
	                }
	            }
	            result = tmp;
	        } catch(Exception nfe) {
	            if (!skipOnError)
	                throw nfe;
	        }
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
