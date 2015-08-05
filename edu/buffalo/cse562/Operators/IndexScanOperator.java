
package edu.buffalo.cse562.Operators;

import com.sleepycat.bind.EntryBinding;
import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.je.*;
import edu.buffalo.cse562.BDBIndexers.BinderWithFilter;
import edu.buffalo.cse562.BDBIndexers.GenericSecKeyCreator;
import edu.buffalo.cse562.BDBIndexers.GenericTupleBinder;
import edu.buffalo.cse562.BL.Evaluator;
import edu.buffalo.cse562.DTO.*;
import edu.buffalo.cse562.Main;
import edu.buffalo.cse562.BDBIndexers.DBManager;
import edu.buffalo.cse562.Util.IndexScanConfig;
import edu.buffalo.cse562.Util.Util;
import net.sf.jsqlparser.expression.BooleanValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.schema.Table;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.*;
import java.util.Map.Entry;

/**
 * @author Shiva
 *
 */
public class IndexScanOperator implements Operator {

    private HashMap<String, ColumnDetail> workingSchema;

    public String getTableName() {
        return tableName;
    }
    private BufferedReader buffer;
    private Path dataFile;
    private String tableName = "";
	private String tableAlias = "";
	private HashMap<String,ColumnDetail> operatorTableSchema = null;
	private Operator parentOperator = null;
	private Expression exp;
    private DiskOrderedCursor mydCursor = null;
	private SecondaryCursor secCursor = null;
	OperationStatus retVal;
	boolean head = true;
	DatabaseEntry foundKey;
	DatabaseEntry foundData;
	ArrayList<Datum> currentTuple;
	DatabaseEntry secondaryKey;
	DatabaseEntry secondaryStart;
	DatabaseEntry secondaryEnd;
	EntryBinding<DataRow> binder;
	HashMap<Integer, String> indexMaps;
    private TreeMap<Integer,Integer> shrinkedIndexMap = null;
    HashMap<String,ColumnDetail> intSchema;
    HashSet<Integer> skipSet = new HashSet<>();


    public void setSearchMode(SearchMode searchMode) {
        this.searchMode = searchMode;
    }

    private SearchMode searchMode;
    Evaluator evaluator;


    public IndexScanOperator(Table table, IndexScanConfig conf){
        /* Constructor for Use with index nested loop joins*/
        //Schema init
        this.tableName = table.getName().toLowerCase();
        this.tableAlias = table.getAlias();
        indexMaps = Main.indexTypeMaps.get(this.tableName.toLowerCase());
        HashMap<String,ColumnDetail> intSchema = Main.tableMapping.get(this.tableName.toLowerCase());
        if (intSchema == null){
            this.tableName = table.getName().toUpperCase();
            intSchema = Main.tableMapping.get(this.tableName.toUpperCase());
            indexMaps = Main.indexTypeMaps.get(this.tableName.toUpperCase());
        }
        this.operatorTableSchema = this.initialiseOperatorTableSchema(intSchema);
        initConfig(conf);
        reset();
    }

    private void initConfig(IndexScanConfig conf) {
        this.searchMode = conf.getSearchMode();
        LeafValue val = conf.getValue();
        this.exp = conf.getExpr();
        if (searchMode.equals(SearchMode.EQ)) {
            this.setSecondaryKey(val);
        }
    }

	@Override
	public ArrayList<Datum> readOneTuple() {
		currentTuple = null;
		foundKey = new DatabaseEntry();
		foundData = new DatabaseEntry();
        if (searchMode.equals(SearchMode.EQ)){
            return readSecondary();
        }
        return readRaw();
	}

    private ArrayList<Datum> readRaw(){
        while (buffer != null) {
            String line = null;
            try {
                line = buffer.readLine();
            } catch (IOException e) {
                //			e.printStackTrace();
                return null;
            }

            if (line == null || line.isEmpty()) {
                return null;
            }

            ArrayList<Datum> tuple = getTuple(line);
            if (checkTuple(tuple)) {
                return tuple;
            }
        }
        return null;
    }

	private ArrayList<Datum> readPrimary(){
        boolean result = false;
        while (mydCursor.getNext(foundKey, foundData, LockMode.READ_UNCOMMITTED) == OperationStatus.SUCCESS){
            currentTuple = binder.entryToObject(foundData).getRow();
            if (checkTuple(currentTuple)){
                return currentTuple;
            }
        }
		return null;
	}

	public ArrayList<Datum> readSecondary(){
        if (head) {
            head = false;
            if (secCursor.getSearchKey(secondaryKey, foundData, LockMode.READ_UNCOMMITTED) == OperationStatus.SUCCESS) {
                currentTuple = binder.entryToObject(foundData).getRow();
                if (checkTuple(currentTuple)) {
                    return currentTuple;
                }
            }
        }

        while ((secCursor.getNextDup(secondaryKey, foundData, LockMode.READ_UNCOMMITTED) == OperationStatus.SUCCESS)){
            currentTuple = binder.entryToObject(foundData).getRow();
            if (checkTuple(currentTuple)){
                return currentTuple;
            }
        }
		return null;
	}

    private boolean checkTuple(ArrayList<Datum> tuple){
        boolean result = true;
        if (exp != null) {
            evaluator = new Evaluator(tuple, this.getOutputTupleSchema());
            BooleanValue bv = null;
            try {
                bv = (BooleanValue) evaluator.eval(exp);
                result = bv.getValue();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return result;
    }

	/* (non-Javadoc)
	 * @see edu.buffalo.cse562.Operator#reset()
	 */
	@Override
	public void reset() {
        mydCursor = DBManager.getInstance().getDiskOrderedCursor(tableName);
        binder = new BinderWithFilter(indexMaps);
        if (Main.secondaryKeyCols.containsKey(tableName)) {
            initSecondaryDb();
        }
        else {
            searchMode = SearchMode.SCAN;
        }
        buffer = DBManager.getInstance().getBuffer(tableName);
    }

    private void initSecondaryDb(){
        try {
            SecondaryConfig secConfig = new SecondaryConfig();
            TupleBinding<DataRow> tBinder = new GenericTupleBinder(indexMaps);
            int secIndexCol = Main.secondaryKeyCols.get(tableName);
            SecondaryKeyCreator secKeyCreator = new GenericSecKeyCreator(tBinder, secIndexCol, indexMaps);
            // Get a secondary object and set the key creator on it.
            secConfig.setKeyCreator(secKeyCreator);
            secCursor = DBManager.getInstance().getSecCursor(secConfig, tableName);
        } catch (Exception e){
            e.printStackTrace();
        }
    }

	public String toString(){
		return "INDEX SCAN ON TABLE " + tableName + " with search mode " + searchMode +" and filter " +exp;
	}

	@Override
	public Operator getChildOp() {
		// TODO Auto-generated method stub
		return null;
	}

	public void setChildOp(Operator child) {
		//null		
	}

	// deep copies the map from static table schema object to operatorTableSchema
	//Replaces table aliases
	private HashMap<String,ColumnDetail> initialiseOperatorTableSchema(HashMap<String,ColumnDetail>  createTableSchemaMap)
	{
		HashMap<String,ColumnDetail> opT = new HashMap<>();
		for(Entry<String, ColumnDetail> es : createTableSchemaMap.entrySet())
		{
			String nameKey = es.getKey();

			if(tableAlias != null)
			{
				if(nameKey.contains("."))
				{
					String[] columnWholeTableName = nameKey.split("\\.");
					nameKey = tableAlias +"."+columnWholeTableName[1];
				}
			}
			opT.put(nameKey,es.getValue().clone());
		}
		return opT;
	}

    public void shrinkSchema() {
        /*
            Extracts only columns used throughout the query and uses them to form a new output schema
         */
        skipSet = new HashSet<>();
        TreeMap<Integer, String> newColdefs = new TreeMap<>();
        HashSet<String> usedSet = getUsedColumns();
        HashMap<String,ColumnDetail> opT = new HashMap<>();
        for(Entry<String, ColumnDetail> es : this.getOutputTupleSchema().entrySet())
        {
            int index = es.getValue().getIndex();
            if (!usedSet.contains(es.getKey())){
                skipSet.add(index);
//                System.out.println(index + " Added to skip indexes");
            }
            else {
                ColumnDetail cd = es.getValue().clone();
                opT.put(es.getKey(), cd);
                newColdefs.put(index, es.getKey());
            }
        }
        int counter = 0;

        for (Map.Entry<Integer, String> entry : newColdefs.entrySet()){
            opT.get(entry.getValue()).setIndex(counter);
            counter += 1;
        }
//        System.out.println("Skip set - " +skipSet);
//        System.out.println("New schema - " +opT);
        this.operatorTableSchema = opT;
        reset();
    }

	@Override
	public HashMap<String, ColumnDetail> getOutputTupleSchema() {
		return operatorTableSchema;
	}

	@Override
	public Operator getParent() {

		return parentOperator;
	}

	@Override
	public void setParent(Operator parent) {
		this.parentOperator = parent;
	}

    @Override
    public HashSet<String> getUsedColumns() {
        HashSet<String> usedColumns = parentOperator.getUsedColumns();
        if (this.exp != null){
            HashSet<String> usedCols = Util.getReferencedColumns(exp);
            usedColumns.addAll(usedCols);
        }
        return usedColumns;
    }

    public void setSelectExpression(Expression expr)
	{
		this.exp = expr;
	}

	public void setStartKey(LeafValue value) {
		/* Constructs the start of the search range to be used for the secondary datatbase
		* for that column
		*/
		secondaryStart = createDbEntry(value);
	}

	public void setEndKey(LeafValue end) {
		/* Constructs the end of the search range to be used for the secondary database if there is a secondary index
		* for that column
		*/
		if (secondaryStart == null){
			secondaryStart = new DatabaseEntry();
		}
		secondaryEnd = createDbEntry(end);
	}

	public void setSecondaryKey(LeafValue value) {
		/* Constructs the search key to be used for the secondary database, if there is a secondary index
		* for that column. The if check may be removed
		*/
		secondaryKey = createDbEntry(value);
        head = true;
	}

    public void setSecondaryKey(Datum datum, String type) {
		/* Constructs the search key to be used for the secondary database, if there is a secondary index
		* for that column. The if check may be removed
		*/
        secondaryKey = createDbEntry(datum, type);
        head = true;
    }

	private DatabaseEntry createDbEntry(LeafValue value){
		DatabaseEntry entry = new DatabaseEntry();
		int secIndexCol = Main.secondaryKeyCols.get(tableName);
		String type = Main.indexTypeMaps.get(tableName).get(secIndexCol);
		Datum searchDatum = new Datum(type, value);
		entry.setData(Util.write(searchDatum, type));
		return entry;
	}

    private DatabaseEntry createDbEntry(Datum datum, String type){
        DatabaseEntry entry = new DatabaseEntry();
        entry.setData(Util.write(datum, type));
        return entry;
    }

	public int compareEntries(DatabaseEntry db1, DatabaseEntry db2){
		byte[] bs1 = db1.getData();
		byte[] bs2 = db2.getData();
		for (int i = 0, j = 0; i < bs1.length && j < bs2.length; i++, j++){
			int a = (bs1[i] & 0xff);
			int b = (bs2[j] & 0xff);
			if (a != b) {
				return a - b;
			}
		}
		return bs1.length - bs2.length;
	}

    public enum SearchMode{
        G,GEQ,M,MEQ,EQ,SCAN;
    }

    public ArrayList<Datum> getTuple(String line)
    {
        String col[] = line.split("\\|");
        ArrayList<Datum> tuples = new ArrayList<Datum>();
        for(int i=0; i<col.length; i++)
        {
            String type = indexMaps.get(i);
            if (!skipSet.contains(i)) {
                tuples.add(new Datum(type, col[i]));
            }
        }
        return tuples;
    }
}