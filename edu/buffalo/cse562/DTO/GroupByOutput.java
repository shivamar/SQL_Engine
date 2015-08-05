/**
 * 
 */
package edu.buffalo.cse562.DTO;

import java.util.ArrayList;

/**
 * @author Sathish
 *
 */
public class GroupByOutput {
	private int count;
	private ArrayList<Datum> outputData;
	
	public GroupByOutput()
	{
		this.setCount(0);
		this.setOutputData(new  ArrayList<Datum>());
	}
	public GroupByOutput(ArrayList<Datum> outputData)
	{
		this.setCount(0);
		this.setOutputData(outputData);
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}

	public ArrayList<Datum> getOutputData() {
		return outputData;
	}

	public void setOutputData(ArrayList<Datum> outputData) {
		this.outputData = outputData;
	}
	
	public GroupByOutput clone()
	{
		GroupByOutput gp = new GroupByOutput();
		gp.count = this.count;
		gp.outputData = new  ArrayList<Datum>();
		
		for(Datum tp: this.outputData)
		{
			gp.outputData.add(tp.cloneTuple(tp));
		}
		
		return gp;
	}

}
