package simpledb.storage;

import simpledb.common.Type;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    private List<TDItem> items;

    public List<TDItem> getItems() {
        return items;
    }

    public void setItems(List<TDItem> items) {
        this.items = items;
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
        return items.iterator();
    }

    private static final long serialVersionUID = 1L;

    /**
     * Constructor.
     * Creates a new, empty TupleDesc.
     */
    public TupleDesc() {}

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here
        items = new ArrayList<>();
        for (int i = 0; i < typeAr.length; i++) {
            TDItem item = new TDItem(typeAr[i], fieldAr[i]);
            items.add(item);
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
        items = new ArrayList<>();
        for (Type type : typeAr) {
            TDItem item = new TDItem(type, null);
            items.add(item);
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return items.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        if (i < 0 || i >= items.size()) {
            throw new NoSuchElementException("position " + i + " is not a valid index");
        }
        return items.get(i).fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
        if (i < 0 || i >= items.size()) {
            throw new NoSuchElementException("position " + i + " is not a valid index");
        }
        return items.get(i).fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
        if (name == null) {
            throw new NoSuchElementException("fieldName " + name + " is not founded");
        }
        for (int i=0; i < items.size(); i++) {
            if (name.equals(items.get(i).fieldName)) {
                return i;
            }
        }
        throw new NoSuchElementException("fieldName " + name + " is not founded");
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        int bytes = 0;
        for (TDItem item:items) {
            bytes += item.fieldType.getLen();
        }
        return bytes;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here
        List<TDItem> items = new ArrayList<>();
        items.addAll(td1.getItems());
        items.addAll(td2.getItems());
        TupleDesc res = new TupleDesc();
        res.setItems(items);
        return res;
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    @Override
    public boolean equals(Object o) {
        // some code goes here
        if (!this.getClass().isInstance(o)) {
            return false;
        }
        TupleDesc desc = (TupleDesc) o;
        List<TDItem> tdItems = desc.getItems();
        if (this.numFields() != tdItems.size()) {
            return false;
        }
        for (int i=0; i < tdItems.size(); i++) {
            if (!this.getFieldType(i).equals(tdItems.get(i).fieldType)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    @Override
    public String toString() {
        // some code goes here
        StringBuilder sb = new StringBuilder();
        for (int i=0; i < items.size(); i++) {
            TDItem item = items.get(i);
            sb.append(item.fieldType).append("(").append(item.fieldName).append(")");

            if (i != items.size() - 1) {
                sb.append(", ");
            }
        }
        return sb.toString();
    }
}
