package edu.buffalo.cse562.Operators;
import edu.buffalo.cse562.DTO.ColumnDetail;
import edu.buffalo.cse562.DTO.ConfigManager;
import edu.buffalo.cse562.BL.Evaluator;
import edu.buffalo.cse562.Test.MiniScan;
import edu.buffalo.cse562.DTO.Operator;
import edu.buffalo.cse562.DTO.Datum;
import edu.buffalo.cse562.DTO.TupleComparator;
import edu.buffalo.cse562.Util.Util;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.OrderByElement;

public class ExternalSortOperator implements Operator {
	Operator child;
	File swapDir;
	LinkedHashMap<Integer, Boolean> sortFields;
	Comparator<ArrayList<Datum>> comp;
	int bufferLength;
	HashMap<String, ColumnDetail> outputSchema;
	private static final int BUFFER_SIZE = 100000000;
	TreeMap<Integer, String> typeMap;
	List<ArrayList<Datum>> workingSet;
	boolean sorted = false;
	MiniScan outputStream;
	boolean ascending;
	ArrayList<Datum> lastFlushed;
	List<OrderByElement> orderByElements;
	Operator parentOperator = null;
	Iterator<ArrayList<Datum>> currIter;
	List<ArrayList<Datum>> nTuples = new ArrayList<>(10);
	
	
	public ExternalSortOperator(Operator child, List<OrderByElement> orderByElements) {
		// TODO Auto-generated constructor stub
//		swapDir = new File(ConfigManager.getSwapDir(), UUID.randomUUID().toString());
//		swapDir = new File(ConfigManager.getSwapDir(), "tmp");
//
//		if (!swapDir.exists()){
//			swapDir.mkdir();
//		}
		
		this.orderByElements = orderByElements;
		setChildOp(child);
        this.bufferLength = BUFFER_SIZE/this.getOutputTupleSchema().size();
    }

    private void initSortElements(){
        this.sortFields = new LinkedHashMap<Integer, Boolean>(orderByElements.size());
        for (OrderByElement ob : orderByElements){
            int index = 0;
            try
            {
                index = Evaluator.getColumnDetail(child.getOutputTupleSchema(),ob.getExpression().toString().toLowerCase()).getIndex();
            }
            catch(Exception ex)
            {
                System.err.println("Error in getting index for column:  " + ob.getExpression().toString().toLowerCase());
                System.err.println("parent: " + this.getParent());
                System.err.println("current: " + this);
                System.err.println("child: " + child);

                throw ex;

            }
            sortFields.put(index, ob.isAsc());
        }


        this.comp = new TupleComparator(sortFields);

        //Number of string objects, not number of tuples
        this.typeMap = new TreeMap<Integer, String>();
        for (ColumnDetail c : outputSchema.values()){
            typeMap.put(c.getIndex(), c.getColumnDefinition().getColDataType().toString().toLowerCase());
        }
    }

	@Override
	public ArrayList<Datum> readOneTuple() {
		// TODO Auto-generated method stub
		ArrayList<Datum> currentTuple;

		
		if (!sorted){
//			System.out.println("Begun sorting...");
			long start = new Date().getTime();
			if (ConfigManager.getSwapDir() == null){
				internalSort();
				Collections.sort(this.workingSet, this.comp);
				currIter = this.workingSet.iterator();
			}
			else{
				twoWaySort();
			}
			sorted = true;
//			System.out.println("==== Sorted in " + ((float) (new Date().getTime() - start)/ 1000) + "s");
		}
//		try {
//			bufferedMerge(0, 60);
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}

		//##########
		//Finally, return tuples from sorted file
		if (ConfigManager.getSwapDir() != null){
			currentTuple = outputStream.readTuple();
			if (currentTuple != null){
				return currentTuple;
			}
		}
		else{
			if (this.currIter.hasNext()){
				return currIter.next();
			}
		}

		return null;
	}
	private void internalSort(){
		ArrayList<Datum> currentTuple;
		this.workingSet = new ArrayList<ArrayList<Datum>>();		
		// First run; sorts input tuples in batches, and writes to separate files on disk
		while((currentTuple = child.readOneTuple())!= null){
			this.workingSet.add(currentTuple);
		}
	}
	private void twoWaySort(){
		ArrayList<Datum> currentTuple;
		this.workingSet = new ArrayList<ArrayList<Datum>>(this.bufferLength);
		int index = 0;
		int nPass = 0;
		File currentFileHandler = getFileHandle(index, nPass);
		
		// First run; sorts input tuples in batches, and writes to separate files on disk
		while((currentTuple = child.readOneTuple())!= null){
			if (addToSet(currentTuple, true, currentFileHandler)){
				index = index + 1;
				currentFileHandler = getFileHandle(index, nPass);
			}
		}
		index = index + 1;
		currentFileHandler = getFileHandle(index, nPass);
		flushWorkingSet(currentFileHandler, true);
		// System.out.println("Working set now " +workingSet.size());
		mergeFull(currentFileHandler, index, nPass);
	}
	
	private void bufferedMerge(int start, int end) throws IOException{
		int n = 8;
		int endLocal = Math.min(start+n, end);
		Path outpath;
		PrintWriter bw;
		Path inpath = new File(this.swapDir, Integer.toString(start)).toPath();
		BufferedReader br = Files.newBufferedReader(inpath, Charset.forName("US-ASCII"));
		while (endLocal - start > 1){
			// Update the output file
			outpath = new File(this.swapDir, "Merged_"+endLocal).toPath();
			bw = new PrintWriter(new BufferedWriter(new FileWriter(outpath.toFile(), true)));
			
			//mergeBlock here
			System.out.println("Merging " + inpath.toString() + " with " + (start + 1) +" to " +endLocal);			
			ArrayList<BufferedReader> buffers = getBuffers(br, start+1, endLocal);
			mergeBuffers(buffers, bw);			
			//advance...
			start = endLocal;
			endLocal = Math.min(start+n, end);
			//writer is the new reader
			inpath = outpath;	
			br = Files.newBufferedReader(inpath, Charset.forName("US-ASCII"));
			bw.close();
		}
		System.out.println("Merged all to " + inpath.toString());						
	}
	
	private ArrayList<BufferedReader> getBuffers(BufferedReader previous, int start, int end) throws IOException{
		ArrayList<BufferedReader> buffers = new ArrayList<BufferedReader>(end - start);
		buffers.add(previous);
		for (int i = start; i <= end; i++){
			Path thisPath = new File(this.swapDir, Integer.toString(i)).toPath();
//			System.out.println("Adding " +thisPath.getName(thisPath.getNameCount() - 1));
			BufferedReader br = Files.newBufferedReader(thisPath, Charset.forName("US-ASCII"));
			buffers.add(br);
		}
		return buffers;
	}
	
	private void mergeBuffers(ArrayList<BufferedReader> buffers, PrintWriter outputBuffer) throws IOException{
		 System.out.println("Beginning merge...");
		 int size = buffers.size();
		 String minElement;
		 ArrayList<Integer> workingSet = new ArrayList<Integer>(); 
		 ArrayList<String> elements = new ArrayList<String>(size); 
		 for (int i = 0; i < size; i++){
			 try {
				String thisLine = buffers.get(i).readLine();
				elements.add(thisLine);
//				System.out.println("Read " + thisLine);
			} catch (NumberFormatException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// What to do when the file is empty
				e.printStackTrace();
			}
		 }
		 
		 
		 
		 LinkedHashMap<Integer, Boolean> sortFieldsInt = new LinkedHashMap<Integer, Boolean>();
		 sortFieldsInt.put(0, true);
		 sortFieldsInt.put(1, false);
		 
		 Comparator<String> compar = new TCompare(sortFieldsInt);
//		 System.out.println(elements);
		 Collections.min(elements, compar);
//		 System.out.println("Minimum is " +minElement);
		 minElement = Collections.min(elements, compar);
		 int numNulls = 0;		 
		 while(numNulls <= size && minElement != "NONE"){
			 for (int i = 0; i < size; i++){
				 // TODO Guard against nulls
				 String thisElement = elements.get(i);
//				 System.out.println("Comparing " +thisElement + " and " +minElement);
					 if (compar.compare(thisElement, minElement) == 0){
						 if (thisElement != "NONE"){
							 outputBuffer.println(thisElement);
							 String s = buffers.get(i).readLine();
//								System.out.println("Writing " + thisElement);
							 if (s == null){
								 numNulls = numNulls + 1;
								 elements.set(i, "NULL");
							 }
							 else{
								 elements.set(i, s);
							 }
						 }
					 }
			 }
			 minElement = Collections.min(elements);
//			 System.out.println("Minimum is " +minElement);
		 }
		 outputBuffer.close();
	}
	
	private void mergeFull(File currentFileHandler, int size, int nPass){		
		//This can be changed to a different base for N-way sort; Merge method also has to be changed
//		// Merging
		File fName1 = getFileHandle(0, nPass);
		for (int s = 0; s <= size; s++){
			File fName2 = getFileHandle(s, nPass);
			File fh = getFileHandle("Merged " + s);
			mergeOnce(fName1, fName2, fh);
			fName1 = fh;
		}
		
		try {
			outputStream = new MiniScan(fName1, typeMap);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	private void mergeOnce(File ifName1, File ifName2, File ofName){
//		System.out.println("merging " +ifName1.getName() +" and " +ifName2.getName());
		try {
			MiniScan left = new MiniScan(ifName1, typeMap);
			MiniScan right = new MiniScan(ifName2, typeMap);
			ArrayList<Datum> leftTup = left.readTuple();
			ArrayList<Datum> rightTup = right.readTuple();

			//Merge procedure
			while (!(leftTup == null) && !(rightTup == null)){
				if (this.comp.compare(leftTup, rightTup) > 0){
					addToSet(leftTup, false, ofName);
					leftTup = left.readTuple(); 
				}
				else{
					addToSet(rightTup, false, ofName);
					rightTup = right.readTuple();
				}
			}

			//flush what's left to disk
			while (leftTup != null){
				addToSet(leftTup, false, ofName);
				leftTup = left.readTuple();
			}

			while (rightTup != null){
				addToSet(rightTup, false, ofName);
				rightTup = right.readTuple();
			}
			left.close();
			right.close();
			//cleanup
			flushWorkingSet(ofName, false);			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
	}
	private boolean addToSet(ArrayList<Datum> toAdd, boolean sort, File currentFileHandle){
		/* adds a tuple to the working set and also returns a flag indicating
		 * whether or not the working set was flushed (this is particularly useful for updating
		 * file indexes)
		 */
		if (workingSet.size() < this.bufferLength){
			workingSet.add(toAdd);
			return false;
		}
		else{
			flushWorkingSet(currentFileHandle, sort);
			workingSet.add(toAdd);	
			return true;
		}
	}

	private boolean flushWorkingSet(File currFileHandle, boolean sorted){
		if (sorted){
			Collections.sort(workingSet, this.comp);
		}
		writeToDisk(workingSet, currFileHandle);
		System.gc();
		// System.out.println("Memory used: " + 
		// 		((Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory())/1000000) + "MB");
		workingSet = new ArrayList<ArrayList<Datum>>(this.bufferLength);
		return true;
	}

	private boolean writeToDisk(List<ArrayList<Datum>> out, File writeDir){
		PrintWriter pw;	

		try {			
			//append to file; useful for merging, and ensures that there is never a fileNotFound exception
			pw = new PrintWriter(new FileWriter(writeDir, true));
			for(ArrayList<Datum> t : out){
				Util.printToStream(t, pw);
			}
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

	//TODO make the buffered writer a singleton object
	private boolean writeOneToDisk(ArrayList<Datum> out, File writeDir){
		PrintWriter pw;	

		try {			
			//append to file; useful for merging, and ensures that there is never a fileNotFound exception
			pw = new PrintWriter(new BufferedWriter(new FileWriter(writeDir, true)));
			Util.printToStream(out, pw);
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

	private File getFileHandle(int index, int nPass){
		String fname = nPass+"-"+index;
		File writeDir = new File(this.swapDir, fname);

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
	
	private File getFileHandle(String fname){
		File writeDir = new File(this.swapDir, fname);

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

	private void replacementSort(){
		ArrayList<Datum> currentTuple = child.readOneTuple();
		int nRun = 1;
		this.workingSet = new LinkedList<ArrayList<Datum>>();
		int flushed = 0;		
		lastFlushed = currentTuple;

		//for subsequent runs
		while (currentTuple != null){
			if (this.comp.compare(currentTuple, lastFlushed) >= 0){
				System.out.println("Inserting one!");
				writeOneToDisk(currentTuple, getFileHandle(0, nRun));
				lastFlushed = currentTuple;
			}

			else{
				workingSet.add(currentTuple);
			}

			if (workingSet.size() - 1 > this.bufferLength){
				nRun = nRun + 1;
				Collections.sort(workingSet, this.comp);			
				System.out.println("Flushing working set " +workingSet.size());
				flushed = appendToOutput(workingSet, nRun);
				System.out.println("Flushed " +flushed+ " tuples");
			}
			currentTuple = child.readOneTuple();
		}

		mergeFull(getFileHandle(0, nRun), nRun, 0);

	}

	private int appendToOutput(List<ArrayList<Datum>> workingSet, int nRun){
		int nFlushed = 0;
		for (int i = 0; i < workingSet.size(); i++){
			ArrayList<Datum> tup = workingSet.get(i);
			//			System.out.println("Comparing " + lastFlushed + " and " +tup);
			if (lastFlushed == null){
				lastFlushed = tup;
			}
			if (this.comp.compare(lastFlushed, tup) <= 0){
				//				System.out.println("YES!!");
				lastFlushed = tup;
				writeOneToDisk(tup, getFileHandle(0, nRun));
				workingSet.remove(i);
				nFlushed = nFlushed + 1;
			}
		}
		return nFlushed;
	}

	@Override
	public void reset() {
		// TODO Auto-generated method stub

	}

	@Override
	public HashMap<String, ColumnDetail> getOutputTupleSchema() {
		// TODO Auto-generated method stub
		return this.outputSchema;
	}	


	public Comparator<ArrayList<Datum>> getComp() {
		return comp;
	}

	@Override
	public Operator getChildOp() {
		// TODO Auto-generated method stub
		return child;
	}

	@Override
	public void setChildOp(Operator child) {
		// TODO Auto-generated method stub
		this.child = child;
		child.setParent(this);
		this.outputSchema = child.getOutputTupleSchema();
        initSortElements();
    }

	@Override
	public Operator getParent() {
		return this.parentOperator;
	}

	@Override
	public void setParent(Operator parent) {
		this.parentOperator = parent;		
	}

    @Override
    public HashSet<String> getUsedColumns() {
        HashSet<String> usedColumns = this.parentOperator.getUsedColumns();
        for (OrderByElement element : orderByElements){
            Expression exp = element.getExpression();
            if (exp instanceof Column){
                usedColumns.add(((Column) exp).getWholeColumnName());
            }
        }
        return usedColumns;
    }

    public String toString(){
		return "External Sort on  " + orderByElements ;
	}

	public List<OrderByElement> getOrderByColumns()
	{
		return this.orderByElements;		
	}
	
	private class TCompare implements Comparator<String>{
		LinkedHashMap<Integer, Boolean> sortFields;
		public TCompare(LinkedHashMap<Integer, Boolean> sortFields){
			this.sortFields = sortFields;
		}

		@Override
		public int compare(String o1, String o2) {
			// TODO Auto-generated method stub
			System.out.println();
			if (o1 == null || o1.equals("NONE")){
				return 1;
			}
			if (o2 == null || o2.equals("NONE")){
				return -1;
			}
			String[] list1 = o1.split("\\|");
			String[] list2 = o2.split("\\|");
			int diff = 0;
			for (Map.Entry<Integer, Boolean> mp : sortFields.entrySet()){
				if (mp.getValue()){
					diff = Integer.parseInt(list1[mp.getKey()]) - Integer.parseInt(list2[mp.getKey()]);
				}
				else {
					diff = Integer.parseInt(list2[mp.getKey()]) - Integer.parseInt(list1[mp.getKey()]);
				}
				
				if (diff != 0){
					return diff;
				}
			}
			return diff;
		}
	}
}
