/**
 * 
 */
package edu.buffalo.cse562;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import net.sf.jsqlparser.expression.Expression;

/**
 * @author The Usual Suspects
 * @Name----------------------EmailAddress
 * Praveen Bellam-------------pbellam@buffalo.edu
 * Jagadeesh Vallabhaneni-----jvallabh@buffalo.edu
 * Saket Adusumilli-----------saketadu@buffalo.edu
 * Anil Nalamalapu------------anilkuma@buffalo.edu
 */
// If streams already sorted improvise.
public class SortMergeJoin implements Operator {
	Operator input1;
	Operator input2;
	ColumnInfo[] schema;
	Expression condition;
	Datum[] tuple1 =null;
	Datum [] tuple2 = null;
	Iterator<Datum[]> iter=null;
	boolean active = false;
	ArrayList<Datum []> tupList = null;
	ArrayList<Expression> whereJoinCondition;
	ArrayList<Integer[]> whereJoinIndexes;
	int ind1=0;
	int ind2=0;
	
	
	public SortMergeJoin(Operator input1, Operator input2, Expression condition){
		this.input1 = input1;
		this.input2 = input2;
		this.condition = condition;
		this.schema = getSchema(input1.getSchema(), input2.getSchema());
		this.tuple2 = input2.readOneTuple();
	}
    
	
	
	@Override
	public Datum[] readOneTuple() {
		
		while(true)
		{
			if(iter == null || !iter.hasNext())
			{
				tuple1 = input1.readOneTuple();
				if(tuple1==null)
					return null;
				 getIter();
				 //reading tuple2 is over
				if(iter == null && tuple2==null)
					return null;
				
				else if(iter==null)
					continue;
			}
			return getTuple(tuple1, iter.next());
		}
		
		
		
	}
	public int compareDatum(Datum dat1, Datum dat2){
		int d1= Integer.parseInt(dat1.element);
		int d2= Integer.parseInt(dat2.element);
		if(d1<d2) return -1;
		else if(d1>d2) return 1;
		else return 0;
	}
	
	public void getIter()
	{
		if(tupList!=null)
		{
			if(compareDatum(tupList.get(0)[ind2],tuple1[ind1])==0)
			{	
				iter =  tupList.iterator();
				return;
			
			}
			if(tuple2==null)
			{
				iter = null;
				return;
			}
		}
		
	
		while(compareDatum(tuple2[ind2],tuple1[ind1]) < 0)
		{
			tuple2 = input2.readOneTuple();
			if(tuple2==null)
			{	iter = null;
				return;
			}
		}
		if(compareDatum(tuple2[ind2],tuple1[ind1]) == 0)
		{
			tupList = new ArrayList<Datum []>();
		while(compareDatum(tuple2[ind2],tuple1[ind1]) == 0)
		{
			tupList.add(tuple2);
			tuple2 = input2.readOneTuple();
			if(tuple2 == null)
				break;
		}
			iter= tupList.iterator();
		}
	
		else
			iter = null;
	}
		
	public Datum[] getTuple( Datum [] tuple1, Datum[] tuple2) {
	
		int len = tuple1.length + tuple2.length;
		Datum[] tuple = new Datum[ len];
		for(int i=0;i< len ; i++  )
		{
			if(i<tuple1.length)
			tuple[i]=tuple1[i];
			else
				tuple[i]=tuple2[i-tuple1.length];	
		}
		return tuple;
	}
	
	
	public ColumnInfo[] getSchema(ColumnInfo[] schema1, ColumnInfo[] schema2){
	    //System.out.println("schema1 length is: "+schema1.length);
		int len = schema1.length + schema2.length;
		schema = new ColumnInfo[len];
		for(int i=0;i< len ; i++  )
		{
			if(i<schema1.length)
			schema[i]=schema1[i];
			else
				schema[i]=schema2[i-schema1.length];	
		}
		return schema;
	}
	

	@Override
	public void reset() {
		input1.reset();	
		input2.reset();
		tuple1=null;
	}

	@Override
	public ColumnInfo[] getSchema() {
		// TODO Auto-generated method stub
		return schema;
	}



	public void buildHash() {
		ind1 = whereJoinIndexes.get(0)[0];
		ind2 = whereJoinIndexes.get(0)[1];
		
	}
}
