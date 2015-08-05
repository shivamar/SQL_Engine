package edu.buffalo.cse562;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

import edu.buffalo.cse562.DTO.*;
import edu.buffalo.cse562.BL.*;
import edu.buffalo.cse562.Util.*;
import edu.buffalo.cse562.Test.MiniScan;
import net.sf.jsqlparser.statement.select.OrderByElement;

public class ExternalSort2 implements Operator {
	Operator child;
	File swapDir;
	LinkedHashMap<Integer, Boolean> sortFields;
	Comparator<ArrayList<Datum>> comp;
	int bufferLength;
	HashMap<String, ColumnDetail> outputSchema;
	private static final int BUFFER_SIZE = 100000;
	TreeMap<Integer, String> typeMap;
	List<ArrayList<Datum>> workingSet;
	boolean sorted = false;
	MiniScan outputStream;
	ArrayList<Datum> lastFlushed;
	List<OrderByElement> orderByElements;
	Operator parentOperator = null;
	Iterator<ArrayList<Datum>> currIter;
	List<ArrayList<Datum>> nTuples = new ArrayList<ArrayList<Datum>>(10);
	
	
	public ExternalSort2(Operator child, List<OrderByElement> orderByElements) {
		// TODO Auto-generated constructor stub
		swapDir = new File(ConfigManager.getSwapDir(), UUID.randomUUID().toString());
//		swapDir = new File(ConfigManager.getSwapDir(), "tmp");

		if (!swapDir.exists()){
			swapDir.mkdir();
		}
		
		this.orderByElements = orderByElements;
		setChildOp(child);
		
		this.sortFields = new LinkedHashMap<Integer, Boolean>(orderByElements.size());

		for (OrderByElement ob : orderByElements){
//			System.out.println(ob);
			//int index = this.outputSchema.get(ob.getExpression().toString()).getIndex();
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
		this.bufferLength = BUFFER_SIZE/ this.getOutputTupleSchema().size();
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
				mainSort();
			}
			sorted = true;
			System.out.println("==== Sorted in " + ((float) (new Date().getTime() - start)/ 1000) + "s");
		}

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
	private void mainSort(){
		ArrayList<Datum> currentTuple;
		this.workingSet = new ArrayList<ArrayList<Datum>>(this.bufferLength);
		int index = 0;
		File currentFileHandler = getFileHandle(index);
		
		// First run; sorts input tuples in batches, and writes to separate files on disk
		while((currentTuple = child.readOneTuple())!= null){
			if (addToSet(currentTuple, true, currentFileHandler)){
				index = index + 1;
				currentFileHandler = getFileHandle(index);
			}
		}
		if (workingSet.size() > 0){
			System.out.println("flushing " +workingSet.size() + " tuples still remaining in the working set");
			currentFileHandler = getFileHandle(index);
			flushWorkingSet(currentFileHandler, true);
		}
		System.out.println("Now merging 0 to " + index);
		try {
			BufferedReader finalOutput = bufferedMerge(0, index);
			this.outputStream = new MiniScan(finalOutput, typeMap);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private File getFileHandle(int index){
		File writeDir = new File(this.swapDir, Integer.toString(index));

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
	
	private BufferedReader bufferedMerge(int start, int end) throws IOException{
		int n = 20;
		int endLocal = Math.min(start+n, end);
		Path outpath;
		PrintWriter bw;
		Path inpath = new File(this.swapDir, Integer.toString(start)).toPath();
		BufferedReader br = Files.newBufferedReader(inpath, Charset.forName("US-ASCII"));
		while (endLocal - start >= 1){
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
		return br;
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
		 ArrayList<String> wSet = new ArrayList<String>(); 
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
				// What to do if the file is empty
				e.printStackTrace();
			}
		 }
		 
		 Comparator<String> compar = new TupleComparator2(this.sortFields, this.typeMap);
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
							 addToWorkingSet(wSet, thisElement, outputBuffer, BUFFER_SIZE);
							 String s = buffers.get(i).readLine();
//								System.out.println("Writing " + thisElement);
							 if (s == null){
								 numNulls = numNulls + 1;
								 elements.set(i, "NONE");
							 }
							 else{
								 elements.set(i, s);
							 }
						 }
					 }
			 }
			 minElement = Collections.min(elements, compar);
//			 System.out.println("Minimum is " +minElement);
		 }
		for (String s : wSet){
			outputBuffer.println(s);
		}
		outputBuffer.close();
	}
	
	private void addToWorkingSet(ArrayList<String> wSet, String toAdd, PrintWriter cpw, int limit){
		/* adds a tuple to the working set and also returns a flag indicating
		 * whether or not the working set was flushed (this is particularly useful for updating
		 * file indexes)
		 */
		if (wSet.size() < limit){
			wSet.add(toAdd);
		}
		else{
			for (String s : wSet){
				cpw.println(s);
			}
			System.gc();
			wSet.add(toAdd);	
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
		// System.out.println("changing child of external sort");
		this.child = child;
		child.setParent(this);
		this.outputSchema = child.getOutputTupleSchema();
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
        return null;
    }

    public String toString(){
		return "External Sort II on  " + orderByElements ;
	}

	public List<OrderByElement> getOrderByColumns()
	{
		return this.orderByElements;		
	}
}
