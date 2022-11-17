package simpledb;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import simpledb.Catalog.Table;

/**
 * The Catalog keeps track of all available tables in the database and their
 * associated schemas.
 * For now, this is a stub catalog that must be populated with tables by a
 * user program before it can be used -- eventually, this should be converted
 * to a catalog that reads a catalog table from disk.
 * 
 * @Threadsafe
 */
public class Catalog {

	

	//HashMap to map the table name to table id
	public HashMap<String,Integer> tableMap;
	
	//List of all the tables present in the entire database
	public List<Table> tableList;
	
	//Hashmap that maps the ID of the file to the DbFile object
	public HashMap<Integer,DbFile> fileMap;
	
	
	
	//A class that represents a table stored in the catalog
		public static class Table implements Serializable{
			public final String tableName;
			
			public final String pkeyField;
			
			public final DbFile file;
			
			public Table(DbFile file, String name, String pkeyField) {
				this.file = file;
				this.tableName = name;
				this.pkeyField = pkeyField;
			}
		}
		
		
		
		//An iterator that iterates over all the table in the catalog
		public Iterator<Table> iterator() {
	        Iterator<Table> tableIterator = new Iterator<Table>() {
	            int currentIndex = 0;
	            @Override
	            public boolean hasNext() {
	                if(currentIndex<tableList.size())
	                    return true;
	                return false;
	            }

	            @Override
	            public Table next() {
	                return tableList.get(currentIndex++);
	            }

	            @Override
	            public void remove() {
	                Iterator.super.remove();
	            }
	        };
	        return tableIterator;
	    }
		
		
		
    /**
     * Constructor.
     * Creates a new, empty catalog.
     */
    public Catalog() {
        // some code goes here
    	tableList = new ArrayList<>();
		tableMap = new HashMap<>();
		fileMap = new HashMap<>();
    }
    
    
    public String getTableNameFromID(int tableId) {
		String tableName = "";
		
		for(Map.Entry mapEntry :tableMap.entrySet()) {
			if(tableId == (int)mapEntry.getValue())
				tableName = (String) mapEntry.getKey();
				break;
		}
		return tableName;
		
	}

    /**
     * Add a new table to the catalog.
     * This table's contents are stored in the specified DbFile.
     * @param file the contents of the table to add;  file.getId() is the identfier of
     *    this file/tupledesc param for the calls getTupleDesc and getFile
     * @param name the name of the table -- may be an empty string.  May not be null.  If a name
     * conflict exists, use the last table to be added as the table for a given name.
     * @param pkeyField the name of the primary key field
     */
    public void addTable(DbFile file, String name, String pkeyField) {
        // some code goes here
    	Table table = new Table(file,name,pkeyField);
        if(tableMap.containsKey(name)) {
            for(int i=0;i<tableList.size();i++) {
                Table table1 = tableList.get(i);
                if(table1.tableName.equalsIgnoreCase(name)){
                    tableMap.put(table1.tableName, file.getId());
                    tableList.set(i,table1);
                    break;
                }
            }
        }else {
            tableList.add(table);
            tableMap.put(table.tableName,table.file.getId());
            fileMap.put(file.getId(),file);
        }
    }

    public void addTable(DbFile file, String name) {
        addTable(file, name, "");
    }
    
    public boolean checkFileIdExists(int tableid) {
        boolean flag = false;
        for(Map.Entry mapEntry : tableMap.entrySet()) {
            if(tableid==(int)mapEntry.getValue()){
                flag = true;
                break;
            }
        }
        return flag;
    }

    /**
     * Add a new table to the catalog.
     * This table has tuples formatted using the specified TupleDesc and its
     * contents are stored in the specified DbFile.
     * @param file the contents of the table to add;  file.getId() is the identfier of
     *    this file/tupledesc param for the calls getTupleDesc and getFile
     */
    public void addTable(DbFile file) {
        addTable(file, (UUID.randomUUID()).toString());
    }

    /**
     * Return the id of the table with a specified name,
     * @throws NoSuchElementException if the table doesn't exist
     */
    public int getTableId(String name) throws NoSuchElementException {
        // some code goes here
    	if(name!=null && !name.equalsIgnoreCase("")) {
            if(!tableMap.containsKey(name)){
                throw new NoSuchElementException();
            }else
                return tableMap.get(name);
        }else {
            throw new NoSuchElementException();
        }
    }

    /**
     * Returns the tuple descriptor (schema) of the specified table
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *     function passed to addTable
     * @throws NoSuchElementException if the table doesn't exist
     */
    public TupleDesc getTupleDesc(int tableid) throws NoSuchElementException {
        // some code goes here
    	if(fileMap.containsKey(tableid)) {
            return fileMap.get(tableid).getTupleDesc();
        }else {
            throw new NoSuchElementException();
        }
    }

    /**
     * Returns the DbFile that can be used to read the contents of the
     * specified table.
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *     function passed to addTable
     */
    public DbFile getDatabaseFile(int tableid) throws NoSuchElementException {
        // some code goes here
    	if(fileMap.containsKey(tableid)) {
            return fileMap.get(tableid);
        }else {
            throw new NoSuchElementException();
        }
    }

    public String getPrimaryKey(int tableid) {
    	String tableName = getTableNameFromID(tableid);
        String pkey = "";
        if(!tableName.equalsIgnoreCase("")) {
            for(Table table : tableList) {
                if(table.tableName.equalsIgnoreCase(tableName))
                    pkey = table.pkeyField;
            }
        }
        return pkey;
    }

    public Iterator<Integer> tableIdIterator() {
    	Iterator<Integer> tableIdIterator = new Iterator<Integer>() {
            int currentIndex = 0;
            @Override
            public boolean hasNext() {
                if(currentIndex<tableList.size())
                    return true;
                return false;
            }

            @Override
            public Integer next() {
                Table table = tableList.get(currentIndex);
                currentIndex+=1;
                return table.file.getId();
            }
        };
        return tableIdIterator;
    }

    public String getTableName(int id) {
        // some code goes here
    	String tableName = "";
        if(fileMap.containsKey(id)) {
           for(Table table : tableList) {
               if(table.file.getId()==id) {
                   tableName = table.tableName;
               }
           }
        }
        return tableName;
    }
    
    /** Delete all tables from the catalog */
    public void clear() {
        // some code goes here
    	tableMap.clear();
        tableList.clear();
        fileMap.clear();
    }
    
    /**
     * Reads the schema from a file and creates the appropriate tables in the database.
     * @param catalogFile
     */
    public void loadSchema(String catalogFile) {
        String line = "";
        String baseFolder=new File(new File(catalogFile).getAbsolutePath()).getParent();
        try {
            BufferedReader br = new BufferedReader(new FileReader(new File(catalogFile)));
            
            while ((line = br.readLine()) != null) {
                //assume line is of the format name (field type, field type, ...)
                String name = line.substring(0, line.indexOf("(")).trim();
                //System.out.println("TABLE NAME: " + name);
                String fields = line.substring(line.indexOf("(") + 1, line.indexOf(")")).trim();
                String[] els = fields.split(",");
                ArrayList<String> names = new ArrayList<String>();
                ArrayList<Type> types = new ArrayList<Type>();
                String primaryKey = "";
                for (String e : els) {
                    String[] els2 = e.trim().split(" ");
                    names.add(els2[0].trim());
                    if (els2[1].trim().toLowerCase().equals("int"))
                        types.add(Type.INT_TYPE);
                    else if (els2[1].trim().toLowerCase().equals("string"))
                        types.add(Type.STRING_TYPE);
                    else {
                        System.out.println("Unknown type " + els2[1]);
                        System.exit(0);
                    }
                    if (els2.length == 3) {
                        if (els2[2].trim().equals("pk"))
                            primaryKey = els2[0].trim();
                        else {
                            System.out.println("Unknown annotation " + els2[2]);
                            System.exit(0);
                        }
                    }
                }
                Type[] typeAr = types.toArray(new Type[0]);
                String[] namesAr = names.toArray(new String[0]);
                TupleDesc t = new TupleDesc(typeAr, namesAr);
                HeapFile tabHf = new HeapFile(new File(baseFolder+"/"+name + ".dat"), t);
                addTable(tabHf,name,primaryKey);
                System.out.println("Added table : " + name + " with schema " + t);
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(0);
        } catch (IndexOutOfBoundsException e) {
            System.out.println ("Invalid catalog entry : " + line);
            System.exit(0);
        }
    }
}

