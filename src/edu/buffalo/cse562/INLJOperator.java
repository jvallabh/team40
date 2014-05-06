package edu.buffalo.cse562;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

import net.sf.jsqlparser.expression.Expression;
import jdbm.RecordManager;
import jdbm.RecordManagerFactory;
import jdbm.btree.BTree;

public class INLJOperator implements Operator {
	
	Operator input1;
	Operator input2;
	static RecordManager indexFile;
	BTree tree;
	ColumnInfo[] schema;
	ArrayList<Expression> whereJoinCondition;
	ArrayList<Integer[]> whereJoinIndexes;
	Iterator<Datum[]> iter=null;
	Datum[] tuple1 =null;
	int ind1;
	int ind2;
	
	public INLJOperator(Operator input1, Operator input2, ArrayList<Integer[]> whereJoinIndexes) {
		this.input1 = input1;
		this.input2 = input2;
		this.schema = getSchema(input1.getSchema(),input2.getSchema());
	}
	
	public void indexLoad() {
		ind1 = this.whereJoinIndexes.get(0)[0];
		ind2 = this.whereJoinIndexes.get(0)[1];
		try{
		tree = BTree.load(indexFile,indexFile.getNamedObject(input2.getSchema()[ind2].origTableName+"_"+input2.getSchema()[ind2].colDef.getColumnName()));
		}
		catch(Exception e) {
			e.printStackTrace();	
		}
	}
	
	@Override
	public Datum[] readOneTuple() {
		/*if (!isHashed)
		{ 
			buildHash();
			isHashed = true;
		}*/
		Datum[] tuple3 = null;
		do {
			if(iter==null)
			{
				tuple1 = input1.readOneTuple();
				if(tuple1==null)
					return null;
				try{
				if(tree.find(tuple1[ind1])==null)
					continue;
				else
				{
					iter =  ((ArrayList<Datum[]>)tree.find(tuple1[ind1])).iterator();
					if(iter.hasNext())
					{
						Object[] obj = iter.next();
						Datum[] tuple4 = Arrays.asList(obj).toArray(new Datum[obj.length]);
						tuple3 = getTuple(tuple1,tuple4);
				        return tuple3;
					}
					else
						iter=null;
				}
			}
				catch(Exception e){
					e.printStackTrace();
				}
			}
			else
			{
				if(iter.hasNext())
				{
					Object[] obj = iter.next();
					Datum[] tuple4 = Arrays.asList(obj).toArray(new Datum[obj.length]);
					tuple3 = getTuple(tuple1, tuple4);
			        return tuple3;
				}
				else
					iter=null;
				
			}
		}while(tuple3==null);
		return null;		
	}

	@Override
	public void reset() {
		// TODO Auto-generated method stub

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
	public ColumnInfo[] getSchema() {
		// TODO Auto-generated method stub
		return schema;
	}

}
