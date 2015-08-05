package edu.buffalo.cse562;
import edu.buffalo.cse562.BDBIndexers.BDBCreator;
import edu.buffalo.cse562.BDBIndexers.DBManager;
import edu.buffalo.cse562.Util.Util;
import edu.buffalo.cse562.DTO.Datum;
import edu.buffalo.cse562.BL.QueryOptimizer;
import edu.buffalo.cse562.DTO.Operator;
import edu.buffalo.cse562.BL.SanitizeQuery;
import edu.buffalo.cse562.DTO.ConfigManager;
import edu.buffalo.cse562.DTO.ColumnDetail;
import edu.buffalo.cse562.Operators.UnionOperator;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringReader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.create.table.Index;
import net.sf.jsqlparser.statement.select.Select;

import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.Union;

import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class Main {

	public static HashMap<String, HashMap<String, ColumnDetail>> tableMapping = new HashMap<String, HashMap<String, ColumnDetail>>();
	public static HashMap<String,HashMap<Integer, String>> indexTypeMaps = new HashMap<String, HashMap<Integer, String>>();
	public static HashMap<String,ArrayList<String>> tableColumns = new HashMap<String, ArrayList<String>>();
	public static HashMap<String,Integer> secondaryKeyCols = new HashMap<>();

	static int queryCount = 0;
	static boolean  tpchexec = true;
	public static void main(String[] args) {		
		if(args.length < 3){
			System.out.println("Incomplete arguments");
			return;
		}
		ArrayList<String> sqlFiles = new ArrayList<>();
		boolean loadPhase = false;

		for (int i = 0; i<args.length; i++) {
			if (args[i].equals("--data")) {
				ConfigManager.setDataDir(args[i + 1]);
			} else if (args[i].equals("--db")) {
				ConfigManager.setDBDir(args[i + 1]);
			} else if (args[i].endsWith(".sql")) {
				sqlFiles.add(args[i]);
			} else if (args[i].equals("--load")) {
				loadPhase = true;
			}
			else if (args[i].equals("--swap")) {
				ConfigManager.setSwapDir(args[i + 1]);
			}
		}

		long start = new Date().getTime();
		processSchema(sqlFiles, loadPhase);
//		System.out.println("==== Operation executed in " + ((float) (new Date().getTime() - start) / 1000) + "s");

//		if (loadPhase){
//			processSchema(sqlFiles, true);
//			new GeneralIndexCreator(ConfigManager.getDbDir());
//			new IndexerTest();
//			return;
//		}
//
//		processSchema(sqlFiles, false);
	}
	
	private static void processSchema(ArrayList<String> sqlQueries, boolean loadPhase) {

		ArrayList<File> queryFiles = new ArrayList<File>();

		for(String q : sqlQueries){
			queryFiles.add(new File(q));
		}

		for (File f : queryFiles) {
			try {
				CCJSqlParser parser = new CCJSqlParser(new FileReader(f));
				ExecuteFile(parser, loadPhase, false);
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} finally {
                DBManager.getInstance().closeAll();
            }
        }

        if (loadPhase){
            try {
                String query = "SELECT * from lineitem";
                CCJSqlParser prs = new CCJSqlParser(new StringReader(query));
                ExecuteFile(prs, false, true);
                System.out.println("Done with lineitem");
                query = "SELECT * from orders";
                prs = new CCJSqlParser(new StringReader(query));
                ExecuteFile(prs, false, true);
                System.out.println("Done with orders");
                query = "SELECT * from customer";
                prs = new CCJSqlParser(new StringReader(query));
                ExecuteFile(prs, false, true);
                System.out.println("Done with customer");
            } catch (Exception e){
                e.printStackTrace();
            } finally {
                DBManager.getInstance().closeAll();
            }
        }
	}

	private static void createBDB(CreateTable createTableObj) {
		/*Creates BDB database and collects stats for each table*/
		String tableName = createTableObj.getTable().getWholeTableName().toLowerCase();
		SanitizeQuery sq = new SanitizeQuery();
		List<ColumnDefinition> cds = (List<ColumnDefinition>) createTableObj.getColumnDefinitions();
		ArrayList<Integer> primaryKeyColumns = new ArrayList<>();
		for (Index ind : (List<Index>)createTableObj.getIndexes()){
			if (ind.getType().equals("PRIMARY KEY")){
				List<String> columnNames = (List<String>) ind.getColumnsNames();
				for (String cName : columnNames) {
					int keyPosition = Main.tableColumns.get(tableName).indexOf(cName.toLowerCase());
					primaryKeyColumns.add(keyPosition);
				}
			}
		}
		if (secondaryKeyCols.containsKey(tableName)) {
			int secondaryCol = secondaryKeyCols.get(tableName);
			System.out.println("Creating BDB for table " + tableName + " with primary keys on columns " +
							primaryKeyColumns + " and secondary index on " + secondaryCol);
			new BDBCreator(tableName, primaryKeyColumns, secondaryCol);
		}
		else{
			System.out.println("Creating BDB for table " + tableName + " with primary keys on columns " +
					primaryKeyColumns + " with no secondary index");
			new BDBCreator(tableName, primaryKeyColumns);
		}
	}

	/**
	 * (non javaDocs)
	 * prepares table schema information and saves it in a static hashmap 
	 * @param createTableObj createTableObject from jsql parser
	 * @author Shiva
	 */
	private static void prepareTableSchema(CreateTable createTableObj){		
		@SuppressWarnings("unchecked")
		String[] tableNames = new String[1];
		String tableName = createTableObj.getTable().getWholeTableName().toLowerCase();
		SanitizeQuery sq = new SanitizeQuery();
		List<ColumnDefinition> cds = (List<ColumnDefinition>) createTableObj.getColumnDefinitions();
		HashMap<String, ColumnDetail> tableSchema = new HashMap<String, ColumnDetail>();
		HashMap<Integer, String> typeInfo = new HashMap<Integer, String>();
		int colCount = 0;
		for(ColumnDefinition colDef : cds){
			ColumnDetail columnDetail = new ColumnDetail();
			columnDetail.setTableName(tableName);
			columnDetail.setColumnDefinition(colDef);
			columnDetail.setIndex(colCount);
			String colname = colDef.getColumnName().toLowerCase();
			String columnFullName = tableName + "."+ colname;

			typeInfo.put(colCount, colDef.getColDataType().getDataType()); //indexMaps : {tableName:{columnIndex:columnType}}

			tableSchema.put(columnFullName, columnDetail);
			sq.addColumnToTable(tableName, colname);
			colCount++;
		}
		tableMapping.put(tableName,tableSchema);
		indexTypeMaps.put(tableName, typeInfo);
		initSecondarykeyCols();
	}

	private static void initSecondarykeyCols() {
		secondaryKeyCols.put("lineitem", 0);
		secondaryKeyCols.put("orders", 1);
		secondaryKeyCols.put("nation", 1);
		secondaryKeyCols.put("customer", 6);
	}

	static void printTuple(ArrayList<Datum> singleTuple) {
		for(int i=0; i < singleTuple.size();i++){

			try
			{
				String str = (singleTuple.get(i)==null)?"":singleTuple.get(i).toString();
				System.out.print(str);
			}
			catch(Exception ex)
			{
				ex.printStackTrace();
				System.out.println(singleTuple.get(i));
			}

			if(i != singleTuple.size() - 1) System.out.print("|");
		}
		System.out.println();
	}	

	static String getTupleAsString(ArrayList<Datum> singleTuple) {

		StringBuilder sb = new  StringBuilder();
		for(int i=0; i < singleTuple.size();i++){

			try
			{
				String tupleStr = (singleTuple.get(i)==null)?"":singleTuple.get(i).toString();
				sb.append(tupleStr);
				//System.out.print(str);
			}
			catch(Exception ex)
			{
				ex.printStackTrace();
				System.out.println(singleTuple.get(i));
			}

			if(i != singleTuple.size() - 1) sb.append("|");
		}
		sb.append("\n");
		return sb.toString();
		// System.out.println();
	}	

	static void ExecuteQuery(Operator op)
	{
		ArrayList<Datum> dt=null;
		StringBuilder sb = new StringBuilder();
		do
		{
			dt = op.readOneTuple();
			if(dt !=null)
			{
//				printTuple(dt);
				sb.append(getTupleAsString(dt));
			}

		}while(dt!=null);

		System.out.print(sb.toString());
	}

    static void ExecuteSilent(Operator op)
    {
        ArrayList<Datum> dt=null;
        StringBuilder sb = new StringBuilder();
        do
        {
            dt = op.readOneTuple();

        }while(dt!=null);
    }

    static void ExecuteQuerytcph10(String selectStr)
	{
		//PlainSelect ps =PlainSelect (PlainSelect) sec;

		// System.err.println("yay");
		String query = "SELECT customer.custkey, sum(lineitem.extendedprice * (1 - lineitem.discount)) AS revenue, customer.acctbal, nation.name FROM customer, orders, lineitem, nation WHERE customer.custkey = orders.custkey AND lineitem.orderkey = orders.orderkey AND orders.orderdate >= Date('[[1]]) AND orders.orderdate < Date('[[2]])       AND lineitem.returnflag = '[[45]]'         AND customer.nationkey = nation.nationkey GROUP BY customer.custkey ORDER BY revenue LIMIT 20";
		Matcher m = Pattern.compile("\\('([^)]+)\\)").matcher(selectStr);
		ArrayList<String> dates = new ArrayList<String>();
		while(m.find()) {
			dates.add(m.group(1));
		}

		//System.out.println(query);
		query = query.replace("[[1]]", dates.get(0)) ;
		selectStr = selectStr.replace("'"+dates.get(0), "");
		selectStr = selectStr.replace("'"+dates.get(1), "");
		//System.out.println(query);

		query = query.replace("[[2]]", dates.get(1)) ;
		//System.out.println(query);


		Pattern pattern = Pattern.compile("'(.*?)'");
		Matcher matcher = pattern.matcher(selectStr);
		if (matcher.find())
		{

			query = query.replace("[[45]]", matcher.group(1).toUpperCase()) ;
		}

		//System.out.println(query);
		try {

			//System.out.println(query);
			CCJSqlParser parser = new CCJSqlParser(new StringReader(query));
			SelectBody select = ((Select) parser.Statement()).getSelectBody();



			SanitizeQuery sq = new SanitizeQuery();
			Operator op = sq.generateTree(select);
			new QueryOptimizer(op);

			// printPlan(op);
			ArrayList<Datum> dt=null;
			StringBuilder sb = new StringBuilder();
			do
			{
				dt = op.readOneTuple();
				if(dt !=null)
				{

					// printTuple(dt);
					writeOneToDisk(dt);
					sb.append(getTupleAsString(dt)); 
				}

			}while(dt!=null);


			String str2 = "CREATE TABLE TEMP ( custkey      INT,      revenue         decimal,         acctbal     decimal, name char(25)); CREATE TABLE CUSTOMER (custkey      INT,name         VARCHAR(25),address      VARCHAR(40),nationkey    INT,phone        CHAR(15),acctbal      DECIMAL,mktsegment   CHAR(10),comment      VARCHAR(117)); SELECT customer.custkey, temp.revenue, temp.acctbal, temp.name , customer.address, customer.phone, customer.comment from Customer,		temp where temp.custkey = customer.custkey";
			CCJSqlParser parser2 = new CCJSqlParser(new StringReader(str2));
			ExecuteFile(parser2, false, true);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}



	}	

	static void printPlan(Operator op)
	{
		while(op!=null)
		{
			System.out.println(op.toString());
			op = op.getChildOp();
		}
	}

	private static  boolean writeOneToDisk(ArrayList<Datum> out){
		PrintWriter pw;	
		File writeDir = getFileHandle();
		try {			
			//append to file; useful for merging, and ensures that there is never a fileNotFound exception
			pw = new PrintWriter(new BufferedWriter(new FileWriter(writeDir, true)));
			Util.printToStream(out, pw);
			//Util.printTuple(out);
			pw.close();
			return true;
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
	}

	private static  File getFileHandle(){
		String fname = "temp.dat";
		File writeDir = new File(ConfigManager.getSwapDir(), fname);

		if (!writeDir.exists()){
			try {
				writeDir.createNewFile();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return writeDir;
	}


	static String readFile(String path, Charset encoding) 
			throws IOException 
	{
		byte[] encoded = Files.readAllBytes(Paths.get(path));
		return new String(encoded, encoding);
	}

	public static void ExecuteFile(CCJSqlParser parser,boolean loadPhase, boolean silent)
	{
		Statement statement;
		SanitizeQuery sq = new SanitizeQuery();
		//ExpressionTree sq = new ExpressionTree();
		try {
			while ((statement = parser.Statement()) != null){
				//					System.out.println(statement);
				if(statement instanceof Select){
					SelectBody select = ((Select) statement).getSelectBody();

					if (select instanceof PlainSelect){
						// 	System.err.println(select);
						// Operator op = e.generateTree(select);
						Operator op = sq.generateTree(select);
						try
						{

							// System.err.println(select.toString());
							//System.out.println("______________________________________");
							//System.out.println("	Old Execution Plan");
							//System.out.println("______________________________________");
//							printPlan(op);
							//System.out.println("______________________________________");
							//System.out.println("	Old Execution Plan's Result");
							//System.out.println("______________________________________");
							long start = new Date().getTime();
//							 ExecuteQuery(op);
							//System.out.println("______________________________________");
							// System.out.println("==== Query executed in " + ((float) (new Date().getTime() - start)/ 1000) + "s");
							//System.out.println("\n\n	Optimized Execution Plan");
							// System.out.println("______________________________________");


							if(ConfigManager.getSwapDir() == null || ConfigManager.getSwapDir().isEmpty()


									) 
							{
								new QueryOptimizer(op);
							}
							else
							{
								// new QueryOptimizer(op);
								new QueryOptimizer(op);
							}

//							 printPlan(op);

							//System.out.println("______________________________________");
							//System.out.println("	Optimized Execution Plan's Result");
							//System.out.println("______________________________________");
							start = new Date().getTime();
                            if (!silent) {
                                ExecuteQuery(op);
                            }
                            else {
                                ExecuteSilent(op);
                            }
//							 System.out.println("==== Query executed in " + ((float) (new Date().getTime() - start)/ 1000) + "s");

						}
						catch(Exception ex)
						{
//							System.err.println("ERROR MSG");
//							System.err.println(select);
//							System.out.println(ex.getMessage());
							ex.printStackTrace();
						}
					}
					else if (select instanceof Union){
						Union un = (Union) select;
						Operator op;
						UnionOperator uop = new UnionOperator();
						List<PlainSelect> pselects = (List<PlainSelect>) un.getPlainSelects();
						for (PlainSelect s : pselects){

							uop.addOperator(sq.generateTree(s));
						}
						ExecuteQuery(uop);
					}

				}
				if(statement instanceof CreateTable){
					CreateTable createTableObj = (CreateTable) statement;
					prepareTableSchema(createTableObj);
					if (loadPhase) {
						createBDB(createTableObj);
					}
                }
			}
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
