/**
 * 
 */
package edu.buffalo.cse562;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import java.util.*;

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
	ColumnDefinition[] schema;
	Expression condition;
	Datum[] tuple1 =null;
	Datum[] tuple2 =null;
	boolean active = false;
	
	public JoinOperator(Operator input1, Operator input2, Expression condition){
		this.input1 = input1;
		this.input2 = input2;
		this.condition = condition;
		this.schema =getSchema(input1.getSchema(), input2.getSchema());
		
		
	}

	@Override
	public Datum[] readOneTuple() {
		Datum[] tuple3 = null;
		do {
			if(tuple2==null)
			{
				tuple1 = input1.readOneTuple();
				if(tuple1==null)
					return null;
				tuple2 = input2.readOneTuple();
			}
			else
				tuple2 = input2.readOneTuple();
		if(tuple1 == null || tuple2 == null){
			continue;
		}
		tuple3 = getTuple(tuple1,tuple2);
		Evaluator eval = new Evaluator(schema, tuple3);
		if(condition != null){
			condition.accept(eval);
			if ((!eval.getBool())) 
				tuple3 = null;			
		}
			
		} while(tuple3 == null);
		return tuple3;
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
	
	
	public ColumnDefinition[] getSchema(ColumnDefinition[] schema1, ColumnDefinition[] schema2){
	    //System.out.println("schema1 length is: "+schema1.length);
		int len = schema1.length + schema2.length;
		schema = new ColumnDefinition[len];
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
	public ColumnDefinition[] getSchema() {
		// TODO Auto-generated method stub
		return schema;
	}
}
