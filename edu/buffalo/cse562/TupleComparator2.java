package edu.buffalo.cse562;

import edu.buffalo.cse562.DTO.Datum;

import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;

public class TupleComparator2 implements Comparator<String> {
	LinkedHashMap<Integer, Boolean> sortFields;
	TreeMap<Integer, String> typeMap;
	public TupleComparator2(LinkedHashMap<Integer, Boolean> sortFields, TreeMap<Integer, String> typeMap){
		this.sortFields = sortFields;
		this.typeMap = typeMap;
	}

	@Override
	public int compare(String o1,String o2) {
		// TODO Auto-generated method stub
//		System.out.println("comparing " + o1 + " and " + o2);
		String[] list1 = o1.split("\\|");
		String[] list2 = o2.split("\\|");
		int diff = 0;
		for (Map.Entry<Integer, Boolean> mp : sortFields.entrySet()){	
			int key = mp.getKey();
			String type = typeMap.get(key);
			if (mp.getValue()){
				if (o1 == null || o1 == "NONE"){
					return 1;
				}
				if (o2 == null || o2 == "NONE"){
					return -1; 
				}
				diff = new Datum(type, list1[key]).compareTo(new Datum(type, list2[key]));
			}
			
			else {
				if (o1 == null || o1 == "NONE"){
					return -1;
				}
				if (o2 == null || o2 == "NONE"){
					return 1;
				}				
				diff = new Datum(type, list2[key]).compareTo(new Datum(type, list1[key]));
			}
			
			if (diff != 0){
				return diff;
			}
		}
		return diff;
	}
}
