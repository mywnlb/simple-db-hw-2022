package simpledb.storage;

import simpledb.common.Type;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {
    private List<Type> types;
    private List<String> fieldNames;
    private List<TDItem> items;

    /**
     * A help class to facilitate organizing the information of each field
     */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         */
        public final Type fieldType;

        /**
         * The name of the field
         */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return An iterator which iterates over all the field TDItems
     * that are included in this TupleDesc
     */
    public Iterator<TDItem> iterator() {
        return items.iterator();
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     *
     * @param typeAr  array specifying the number of and types of fields in this
     *                TupleDesc. It must contain at least one entry.
     * @param fieldAr array specifying the names of the fields. Note that names may
     *                be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        if (typeAr == null || typeAr.length == 0) {
            throw new IllegalArgumentException("Type array must contain at least one entry");
        }
        if (fieldAr != null && fieldAr.length != typeAr.length) {
            throw new IllegalArgumentException("Field names array must match the length of type array");
        }

        this.types = Arrays.asList(typeAr);
        this.fieldNames = fieldAr != null ? Arrays.asList(fieldAr) : Arrays.asList(new String[typeAr.length]);

        // Create TDItems
        this.items = Arrays.asList(new TDItem[typeAr.length]);
        for (int i = 0; i < typeAr.length; i++) {
            items.set(i, new TDItem(typeAr[i], fieldNames.get(i)));
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     *
     * @param typeAr array specifying the number of and types of fields in this
     *               TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        if (typeAr == null || typeAr.length == 0) {
            throw new IllegalArgumentException("Type array must contain at least one entry");
        }

        this.types = Arrays.asList(typeAr);
        this.fieldNames = Arrays.asList(new String[typeAr.length]);

        // Create TDItems with null names
        this.items = Arrays.asList(new TDItem[typeAr.length]);
        for (int i = 0; i < typeAr.length; i++) {
            items.set(i, new TDItem(typeAr[i], null));
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        return fieldNames.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     *
     * @param i index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        if (i < 0 || i >= fieldNames.size()) {
            throw new NoSuchElementException("Index " + i + " is out of bounds for TupleDesc with " + fieldNames.size() + " fields.");
        }
        return fieldNames.get(i);
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     *
     * @param i The index of the field to get the type of. It must be a valid
     *          index.
     * @return the type of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        if (i < 0 || i >= types.size()) {
            throw new NoSuchElementException("Index " + i + " is out of bounds for TupleDesc with " + types.size() + " fields.");
        }
        return types.get(i);
    }

    /**
     * Find the index of the field with a given name.
     *
     * @param name name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException if no field with a matching name is found.
     */
    public int indexForFieldName(String name) throws NoSuchElementException {
        for (int i = 0; i < fieldNames.size(); i++) {
            if (fieldNames.get(i) != null && fieldNames.get(i).equals(name)) {
                return i;
            }
        }
        throw new NoSuchElementException("No field with name " + name + " found in TupleDesc.");
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     * Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        int size = 0;
        for (Type type : types) {
            size += type.getLen();
        }
        return size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     *
     * @param td1 The TupleDesc with the first fields of the new TupleDesc
     * @param td2 The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        if (td1 == null && td2 == null) {
            throw new IllegalArgumentException("Both TupleDescs cannot be null");
        }
        if (td1 == null) {
            return td2;
        }
        if (td2 == null) {
            return td1;
        }

        Type[] mergedTypes = new Type[td1.numFields() + td2.numFields()];
        String[] mergedNames = new String[mergedTypes.length];

        int index = 0;
        for (int i = 0; i < td1.numFields(); i++) {
            mergedTypes[index] = td1.getFieldType(i);
            mergedNames[index] = td1.getFieldName(i);
            index++;
        }
        for (int i = 0; i < td2.numFields(); i++) {
            mergedTypes[index] = td2.getFieldType(i);
            mergedNames[index] = td2.getFieldName(i);
            index++;
        }

        return new TupleDesc(mergedTypes, mergedNames);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     *
     * @param o the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof TupleDesc)) {
            return false;
        }
        TupleDesc other = (TupleDesc) o;

        if (this.numFields() != other.numFields()) {
            return false;
        }

        for (int i = 0; i < this.numFields(); i++) {
            if (!this.getFieldType(i).equals(other.getFieldType(i))) {
                return false;
            }
            String thisName = this.getFieldName(i);
            String otherName = other.getFieldName(i);
            if ((thisName == null && otherName != null) || (thisName != null && !thisName.equals(otherName))) {
                return false;
            }
        }
        return true;
    }

    public int hashCode() {
        int hash = 1;
        for (int i = 0; i < numFields(); i++) {
            hash = 31 * hash + getFieldType(i).hashCode();
            String fieldName = getFieldName(i);
            hash = 31 * hash + (fieldName != null ? fieldName.hashCode() : 0);
        }
        return hash;
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     *
     * @return String describing this descriptor.
     */
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < numFields(); i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(getFieldType(i).toString());
            String fieldName = getFieldName(i);
            if (fieldName != null) {
                sb.append("(").append(fieldName).append(")");
            } else {
                sb.append("()");
            }
        }
        return sb.toString();
    }
}
