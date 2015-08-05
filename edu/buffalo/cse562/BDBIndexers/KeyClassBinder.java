package edu.buffalo.cse562.BDBIndexers;

import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;

public class KeyClassBinder extends TupleBinding<KeyClass> {
	@Override
	public KeyClass entryToObject(TupleInput tupleInput) {		
		return  new KeyClass(tupleInput.readLong());
	}

	@Override
	public void objectToEntry(KeyClass keyClass_Param, TupleOutput tupleOutput) {
		tupleOutput.writeLong(keyClass_Param.getKey());
	}
	
}
