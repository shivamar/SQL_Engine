package edu.buffalo.cse562.DTO;

import edu.buffalo.cse562.DTO.Datum;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.Map;

public class TupleComparator implements Comparator<ArrayList<Datum>>{
	LinkedHashMap<Integer, Boolean> sortFields;
	public TupleComparator(LinkedHashMap<Integer, Boolean> sortFields){
		this.sortFields = sortFields;
	}

	@Override
	public int compare(ArrayList<Datum> o1,
			ArrayList<Datum> o2) {
		// TODO Auto-generated method stub
		
		if (o1 == null){
			return 1;
		}
		if (o2 == null){
			return -1;
		}
		
		int diff = 0;
		for (Map.Entry<Integer, Boolean> mp : sortFields.entrySet()){
			if (mp.getValue()){
				diff = o1.get(mp.getKey()).compareTo(o2.get(mp.getKey()));
			}
			else {
				diff = o2.get(mp.getKey()).compareTo(o1.get(mp.getKey()));
			}
			
			if (diff != 0){
				return diff;
			}
		}
		return diff;
	}
}