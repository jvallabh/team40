/**
 * 
 */
package edu.buffalo.cse562;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;

/**
 * @author The Usual Suspects
 * @Name----------------------EmailAddress
 * Praveen Bellam-------------pbellam@buffalo.edu
 * Jagadeesh Vallabhaneni-----jvallabh@buffalo.edu
 * Saket Adusumilli-----------saketadu@buffalo.edu
 * Anil Nalamalapu------------anilkuma@buffalo.edu
 */
public class ScanOperator implements Operator {

	BufferedReader input;
	File f;
	ColumnInfo[] schema;
	ArrayList<Expression> conditions = new ArrayList<Expression>();
	Evaluator eval;
	Iterator<Expression> iterator;
	Expression currCondition;
	Datum date1;
	Datum date2;
	
	public ScanOperator(File f, ColumnInfo[] schema) {
		this.f = f;
		this.schema = schema;
		reset(); 
		this.eval = new Evaluator(schema);
	}
	
	@Override
	public Datum[] readOneTuple() {
		if (input == null) return null;
		String line = null;
		Datum[] ret = null;
		while(true){
			try {
				line = input.readLine();
			} catch(IOException e) {
				e.printStackTrace();
			}
			if (line == null) return null;
			String[] cols = line.split("\\|");
			ret = new Datum[cols.length];
			for (int i=0; i<cols.length; i++) {
				//ret[i] = new Datum.Long(cols[i]);
				ret[i] = new Datum(cols[i]);
			}
			if(evaluateTuple(ret)){
				break;
			}			
		}
		return ret;
	}


	@Override
	public void reset() {
		try {
			if(input != null){
				input.close();
			}
			input = new BufferedReader(new FileReader(f));
		} catch (IOException e) {
			e.printStackTrace();
			input = null;
		}
	}

	@Override
	public ColumnInfo[] getSchema() {
		// TODO Auto-generated method stub
		return schema;
	}
	
	public void setVar(){
		iterator = conditions.iterator();
		while(iterator.hasNext()){
			currCondition = iterator.next();				
			((MinorThanEquals)currCondition).getRightExpression().accept(eval);
			date1 = new Datum(eval.getValue());
			currCondition = iterator.next();
			((GreaterThanEquals)currCondition).getRightExpression().accept(eval);	
			date2 = new Datum(eval.getValue());
		}
	}
	
	private boolean evaluateTuple(Datum[] tuple){
		boolean result = true;
		if(Main.tpch&&!Main.build){
		if(!(tuple[3].compareTo(date1)<=0&&tuple[3].compareTo(date2)>=0))
			result = false;
		}
		else if(conditions.size() != 0){
			iterator = conditions.iterator();
			while(iterator.hasNext()){
				currCondition = iterator.next();				
				eval.sendTuple(tuple);
				currCondition.accept(eval);
					if (!(eval.getBool())) {
						result = false;
						break;
					}				
			}
		}
		return result;
	}
}
