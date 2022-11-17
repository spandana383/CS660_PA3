package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

	//make a list that contains a list of all the items in the TupleDesc object
	public List<TDItem> tdItemList = new ArrayList<>();
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

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
    	Iterator<TDItem> tdItemIter = new Iterator<TupleDesc.TDItem>() {
            int currentIndex = 0;
            @Override
            public boolean hasNext() {
                if(currentIndex<tdItemList.size())
                    return true;
                return false;
            }

            @Override
            public TDItem next() {
                return tdItemList.get(currentIndex++);
            }

            @Override
            public void remove() {
                Iterator.super.remove();
            }
        };
        return tdItemIter;
    }

    private static final long serialVersionUID = 1L;

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
    	if(typeAr.length<1) {
            throw new RuntimeException("Invalid array length");
        }
        for(int i=0;i<typeAr.length;i++) {
            if(fieldAr[i]==null) {
                TDItem tdItem = new TDItem(typeAr[i],"");
                tdItemList.add(tdItem);
            }else {
                TDItem tdItem = new TDItem(typeAr[i],fieldAr[i]);
                tdItemList.add(tdItem);
            }
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
    	if(typeAr.length>0) {
            for(Type type : typeAr) {
                TDItem tdItem = new TDItem(type,"");
                tdItemList.add(tdItem);
            }
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return tdItemList.size();
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
    	if(i<tdItemList.size() && i>=0) {
    		return tdItemList.get(i).fieldName;
    	}
    	else {
    		throw new NoSuchElementException();
    	}
        
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
        if(i<tdItemList.size() && i>=0) {
        	return tdItemList.get(i).fieldType;
        }
        else {
        	throw new NoSuchElementException();
        }
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
    	if(name == null) {
    		throw new NoSuchElementException();
    	}
    	else {
    		for(int i=0;i<tdItemList.size();i++) {
    			if(tdItemList.get(i).fieldName.equalsIgnoreCase(name)) {
    				return i;
    			}
    		}
    	}
    	throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        int size = 0;
        for(int i=0;i<tdItemList.size();i++) {
        	size += tdItemList.get(i).fieldType.getLen();
        	
        }
        return size;
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
    	int len = td1.numFields() + td2.numFields();
    	Type[] types = new Type[len];
    	String[] fields = new String[len];
    	
    	
    	for(int i=0;i<len;i++) {
    		if(i<td1.numFields()) {
    			types[i] = td1.getFieldType(i);
    			fields[i] = td1.getFieldName(i);
    		}
    		else {
    			types[i] = td2.getFieldType(i- td1.numFields());
    			fields[i] = td2.getFieldName(i- td1.numFields());
    			
    		}
    	}
        return new TupleDesc(types,fields);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they are the same size and if the n-th
     * type in this TupleDesc is equal to the n-th type in td.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
        // some code goes here
    	if(o!=null && (o instanceof TupleDesc) && ((TupleDesc)o).numFields()==this.numFields()) {
    		for(int index=0;index<this.numFields();index++) {
    			if(tdItemList.get(index).fieldType != ((TupleDesc)o).getFieldType(index)) {
    				return false;
    			}
    		}
    	}
    	else {
    		return false;
    	}
        return true;
    }

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
    public String toString() {
        // some code goes here
    	int i=0;
        String str = "";
        for (TDItem tdItem : tdItemList) {
            if(i<tdItemList.size()-1){
                str += tdItem.toString()+",";
                i+=1;
            }
            else if(i == tdItemList.size()){
                str += tdItem.toString();
            }
        }
        return str;
        
    }
}
