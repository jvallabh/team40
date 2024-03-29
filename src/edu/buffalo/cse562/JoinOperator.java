/**
 * 
 */
package edu.buffalo.cse562;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import jdbm.PrimaryTreeMap;
import net.sf.jsqlparser.expression.Expression;

/**
 * @author The Usual Suspects
 * @Name----------------------EmailAddress
 * Praveen Bellam-------------pbellam@buffalo.edu
 * Jagadeesh Vallabhaneni-----jvallabh@buffalo.edu
 * Saket Adusumilli-----------saketadu@buffalo.edu
 * Anil Nalamalapu------------anilkuma@buffalo.edu
 */
public class JoinOperator implements Operator {
	Operator input1;
	Operator input2;
	ColumnInfo[] schema;
	Expression condition;
	Datum[] tuple1 =null;
	Iterator<Datum[]> iter=null;
	Iterator<String> iter1=null;
	boolean active = false;
	boolean isHashed = false;
	ArrayList<Expression> whereJoinCondition;
	ArrayList<Integer[]> whereJoinIndexes;
	int ind1=0;
	int ind2=0;
	Map<String, List<Datum[]>> map = new HashMap<String, List<Datum[]>>();
	PrimaryTreeMap<String, ArrayList<String>> scanmap;
	boolean doIndexScan=false;
	
	public JoinOperator(Operator input1, Operator input2, Expression condition){
		this.input1 = input1;
		this.input2 = input2;
		this.condition = condition;
		this.schema = getSchema(input1.getSchema(), input2.getSchema());
	
	}
    
	public void buildHash()
	{
		ind1 = whereJoinIndexes.get(0)[0];
		ind2 = whereJoinIndexes.get(0)[1];
		Datum[] tuple3 = input1.readOneTuple();
		while(tuple3!=null)
		{
			if(input1.getSchema()[0].tableName.equals(input1.getSchema()[input1.getSchema().length-1].tableName)&&!input1.getSchema()[3].tableName.equals("lineitem")&&Main.tpch){
				if(input1.getSchema().length==4&&input1.getSchema()[0].origTableName.equals("nation"))
					scanmap = Main.indexFile.treeMap(input1.getSchema()[ind1].origTableName+"_"+input1.getSchema()[ind1].colDef.getColumnName());
				else {
			try{
			scanmap = ((IndexScanOperator)input1).indexFile.treeMap(input1.getSchema()[ind1].tableName+"_"+input1.getSchema()[ind1].colDef.getColumnName());
			}
			catch(Exception e){
				e.printStackTrace();
			}
			}
				doIndexScan = true;
				return;
			}
			else{
			if(map.containsKey(tuple3[ind1].element)) {
		        //Add to existing list
		        map.get(tuple3[ind1].element).add(tuple3);

		    } else {
		        //Create new list
		        List<Datum[]> tuples = new ArrayList<Datum[]>();
		        tuples.add(tuple3);
		        map.put(tuple3[ind1].element, tuples);
		    }
		  tuple3 = input1.readOneTuple();
			}
		}
		

		/*ind1 = whereJoinIndexes.get(0)[0];
		ind2 = whereJoinIndexes.get(0)[1];
		Datum[] tuple3 = input1.readOneTuple();
		while(tuple3!=null)
		{
			if(map.containsKey(tuple3[ind1].element)) {
		        //Add to existing list
		        map.get(tuple3[ind1].element).add(tuple3);

		    } else {
		        //Create new list
		        List<Datum[]> tuples = new ArrayList<Datum[]>();
		        tuples.add(tuple3);
		        map.put(tuple3[ind1].element, tuples);
		    }
		  tuple3 = input1.readOneTuple();
		}*/
	}
	
	@Override
	public Datum[] readOneTuple() {
		/*if (!isHashed)
		{ 
			buildHash();
			isHashed = true;
		}*/
		if(doIndexScan){
			Datum[] tuple3 = null;
			do {
				if(iter1==null)
				{
					tuple1 = input2.readOneTuple();
					if(tuple1==null)
						return null;
					if(!scanmap.containsKey(tuple1[ind2].element))
						continue;
					else
					{
						try{
						iter1 = scanmap.find(tuple1[ind2].element).iterator();
						}
						catch(Exception e){
							e.printStackTrace();
						}
						if(iter1.hasNext())
						{
							tuple3 = getTuple(toDatum(iter1.next()), tuple1);
					        return tuple3;
						}
						else
							iter1=null;
					}
				}
				else
				{
					if(iter1.hasNext())
					{
						tuple3 = getTuple(toDatum(iter1.next()), tuple1);
				        return tuple3;
					}
					else
						iter1=null;
					
				}
			}while(tuple3==null);
			return null;	
			}
	else{
		Datum[] tuple3 = null;
		do {
			if(iter==null)
			{
				tuple1 = input2.readOneTuple();
				if(tuple1==null)
					return null;
				if(!map.containsKey(tuple1[ind2].element))
					continue;
				else
				{
					iter =  map.get(tuple1[ind2].element).iterator();
					if(iter.hasNext())
					{
						tuple3 = getTuple(iter.next(), tuple1);
				        return tuple3;
					}
					else
						iter=null;
				}
			}
			else
			{
				if(iter.hasNext())
				{
					tuple3 = getTuple(iter.next(), tuple1);
			        return tuple3;
				}
				else
					iter=null;
				
			}
		}while(tuple3==null);
		return null;		
	}
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
	
	public Datum[] toDatum(String line){
		String[] cols = line.split("\\|");
		Datum[] ret = new Datum[cols.length];
		for (int i=0; i<cols.length; i++) {
			//ret[i] = new Datum.Long(cols[i]);
			ret[i] = new Datum(cols[i]);
		}
		return ret;
	}
}
