package edu.buffalo.cse562.DTO;

import java.util.ArrayList;


public class DataRow {
	ArrayList<Datum> row = null;
	
	public DataRow(ArrayList<Datum> thisRow){
		row = thisRow;
	}
	
	public ArrayList<Datum> getRow(){
		return this.row;
	}	
}
