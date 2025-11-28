package simpledb.storage;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;

    private TupleDesc tupleDesc; // The schema of this tuple
    private RecordId recordId;
    private Field[] fields; // Array of fields representing the data in this tuple

    /**
     * Create a new tuple with the specified schema (type).
     *
     * @param td the schema of this tuple. It must be a valid TupleDesc
     *           instance with at least one field.
     */
    public Tuple(TupleDesc td) {
        if (td == null || td.numFields() == 0) {
            throw new IllegalArgumentException("TupleDesc must not be null and must have at least one field.");
        }
        this.tupleDesc = td;
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        return this.tupleDesc;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     */
    public RecordId getRecordId() {
        return this.recordId;
    }

    /**
     * Set the RecordId information for this tuple.
     *
     * @param rid the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        if (rid == null) {
            throw new IllegalArgumentException("RecordId must not be null.");
        }
        this.recordId = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     *
     * @param i index of the field to change. It must be a valid index.
     * @param f new value for the field.
     */
    public void setField(int i, Field f) {
        if (i < 0 || i >= tupleDesc.numFields()) {
            throw new IndexOutOfBoundsException("Field index out of bounds: " + i);
        }
        if (fields == null) {
            fields = new Field[tupleDesc.numFields()];
        }
        fields[i] = f;
    }

    /**
     * @param i field index to return. Must be a valid index.
     * @return the value of the ith field, or null if it has not been set.
     */
    public Field getField(int i) {
        if (i < 0 || i >= tupleDesc.numFields()) {
            throw new IndexOutOfBoundsException("Field index out of bounds: " + i);
        }
        if (fields == null || fields[i] == null) {
            return null; // Field has not been set
        }
        return fields[i];
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     * <p>
     * column1\tcolumn2\tcolumn3\t...\tcolumnN
     * <p>
     * where \t is any whitespace (except a newline)
     */
    public String toString() {
        if (fields == null || fields.length == 0) {
            return ""; // No fields set
        }
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < fields.length; i++) {
            if (i > 0) {
                sb.append("\t"); // Append tab between fields
            }
            Field field = fields[i];
            if (field != null) {
                sb.append(field.toString());
            } else {
                sb.append("null"); // Handle unset fields
            }
        }
        return sb.toString();
    }

    /**
     * @return An iterator which iterates over all the fields of this tuple
     */
    public Iterator<Field> fields() {
        if (fields == null) {
            return Arrays.asList(new Field[tupleDesc.numFields()]).iterator(); // Return empty iterator if no fields set
        }
        return Arrays.asList(fields).iterator();
    }

    /**
     * reset the TupleDesc of this tuple (only affecting the TupleDesc)
     */
    public void resetTupleDesc(TupleDesc td) {
        if (td == null || td.numFields() == 0) {
            throw new IllegalArgumentException("TupleDesc must not be null and must have at least one field.");
        }
        this.tupleDesc = td;
        this.fields = new Field[td.numFields()]; // Reset fields to match new TupleDesc
    }
}
