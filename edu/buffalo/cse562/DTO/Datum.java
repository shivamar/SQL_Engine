package edu.buffalo.cse562.DTO;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.LeafValue.InvalidLeaf;


public class Datum implements Comparable<Datum> {
	public LeafValue val ;
	
	//in extended projection while evaluating expressions, we dont get type
	
	public Datum()
	{
		
	}
	public Datum(LeafValue value){
		this.val = value;
	}
	
	public void Update(Datum datum)
	{
		this.val = datum.val;
	}

	public Datum(String type, String colItem){
		try
		{
		switch(type){
			case "int":
			case "INT":
			case "INTEGER":
			case "integer":
			{
				val =  new LongValue(colItem);
				break;
			}
			case "string":
			case "varchar":
			case "char":
			case "STRING":
			case "VARCHAR":
			case "CHAR":
			{
				val =  new StringValue("'"+colItem+"'");
				break;
			}
			case "decimal":
			case "DECIMAL":
			{
				val =  new DoubleValue(colItem);
				break;
			}
			case "date":
			case "DATE":
			{
				if (!colItem.startsWith("'")) {
					val = new DateValue("'" + colItem + "'");
				}
				else {
					val = new DateValue(colItem);
				}
				break;
			}
			default:
			{
				val =  new StringValue("'"+colItem+"'");
			}
		}	
		}
		catch(Exception ex)
		{
			System.out.println("type: "+ type + "colItem: " + colItem);
			ex.printStackTrace();
		}
	}	
	
	 @Override 
	 public String toString()
	 {
		 if(val instanceof StringValue)
		 {
			 return val.toString().substring(1,val.toString().length()-1);
		 }
		 else
			 
		return (val==null)?null: val.toString();		 	 
	 }
	 
	public LeafValue getValue()
	{
		return this.val;
	}

	public Datum add(Datum datum)
	{
		if(this.val instanceof LongValue)
		{
			long longVal =  ((LongValue)this.val).getValue() + ((LongValue)datum.getValue()).getValue();
			((LongValue) val).setValue(longVal);
			
		}

		if(this.val instanceof DoubleValue)
		{
			double doubleVal =  ((DoubleValue)this.val).getValue() + ((DoubleValue)datum.getValue()).getValue();
			((DoubleValue) val).setValue(doubleVal);
		}
		
		return this;

	}
	
	public Datum divideBy(Datum datum)
	{
		if(this.val instanceof LongValue)
		{
			long longVal =  ((LongValue)this.val).getValue() / ((LongValue)datum.getValue()).getValue();
			((LongValue) val).setValue(longVal);
		}

		if(this.val instanceof DoubleValue)
		{
			double doubleVal =  ((DoubleValue)this.val).getValue() / ((LongValue)datum.getValue()).getValue();
			((DoubleValue) val).setValue(doubleVal);
		}
		
		return this;
	}
	
	public boolean isGreaterThan(Datum datum)
	{
		
		if(this.val instanceof LongValue)
		{
			return ((LongValue)this.val).getValue() > ((LongValue)datum.getValue()).getValue();
			
		}

		if(this.val instanceof DoubleValue)
		{
			return  ((DoubleValue)this.val).getValue() > ((DoubleValue)datum.getValue()).getValue();
			
		}
		
		if(this.val instanceof DateValue)
		{
			return  ((DateValue)this.val).getValue().after( ((DateValue)datum.getValue()).getValue());
			
		}
		
		return false;
		
	}
	
	public boolean isLessThan(Datum tup)
	{
		if(this.val instanceof LongValue)
		{
			return ((LongValue)this.val).getValue() < ((LongValue)tup.getValue()).getValue();
			
		}

		if(this.val instanceof DoubleValue)
		{
			return  ((DoubleValue)this.val).getValue() < ((DoubleValue)tup.getValue()).getValue();
			
		}
		
		if(this.val instanceof DateValue)
		{
			return  ((DateValue)this.val).getValue().before(((DateValue) tup.getValue()).getValue());
			
		}
		return false;		
	}	
	
	@Override
	public boolean equals(Object obj) {
		// TODO Auto-generated method stub
		if (obj instanceof Datum){
			if (this.toString().compareTo(((Datum) obj).toString()) == 0){
				return true;
			}
		}
		return super.equals(obj);
	}
	
	public int compareTo(Datum nxtTuple) 
	{
		try 
		{
			  // Only 4 Known types! in Project 1
				if(this.val instanceof StringValue)
				{			
					return (this.val.toString()).compareTo(nxtTuple.val.toString());
				}		
				if(this.val instanceof DoubleValue)
				{			
						return Double.compare(this.val.toDouble(), nxtTuple.val.toDouble());//(this.val.toDouble()).compareTo(nxtTuple.val.toDouble());
				}
				if(this.val instanceof LongValue)
				{
					int returnVal;
					try{
						returnVal = Long.compare(this.val.toLong(), nxtTuple.val.toLong());
					}
					catch (Exception ex){
						returnVal = Long.compare(this.val.toLong(), 
								Long.parseLong(nxtTuple.val.toString().replace("\'", "")));
					}
					return returnVal;
				}
				
				if(this.val instanceof DateValue)
				{
					DateFormat sf = new SimpleDateFormat("yyyy-MM-dd");	
					
					Date dateCurr = sf.parse(this.val.toString());
					Date dateNxt =  sf.parse(nxtTuple.val.toString());
	
						 return dateCurr.compareTo(dateNxt);				 			
				}			
		}
		catch(ParseException | InvalidLeaf e)
		{
			e.printStackTrace();
		}
		return 0;
	}	
	
	public Datum cloneTuple(Datum t)
	{
		if( t.val instanceof StringValue)
		{
			return new Datum("string",t.val.toString().substring(1,val.toString().length()-1));
		}
		if( t.val instanceof DoubleValue)
		{
			return new Datum("decimal",t.val.toString());
		}
		if( t.val instanceof LongValue)
		{
			return new Datum("int",t.val.toString());
		}
		if( t.val instanceof DateValue)
		{
			return new Datum("date",t.val.toString());
		}
		return null;
	}

	public Datum(String type, LeafValue leafValue){
		try
		{
			switch(type){
				case "date":
				case "DATE":
				{
					if (leafValue instanceof StringValue) {
						String dateString = leafValue.toString();
						if (!dateString.startsWith("'")) {
							val = new DateValue("'" + dateString + "'");
						} else {
							val = new DateValue(dateString);
						}
						break;
					}
				}
				default:
				{
					val =  leafValue;
				}
			}
		}
		catch(Exception ex)
		{
			System.out.println("type: "+ type + "leafValue: " + leafValue);
			ex.printStackTrace();
		}
	}
}