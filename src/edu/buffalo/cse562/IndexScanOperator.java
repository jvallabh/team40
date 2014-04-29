/**
 * 
 */
package edu.buffalo.cse562;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import jdbm.RecordManager;
import jdbm.btree.BTree;
import jdbm.btree.BTree;
import jdbm.helper.TupleBrowser;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.schema.Column;

/**
 * @author The Usual Suspects
 * @Name----------------------EmailAddress
 * Praveen Bellam-------------pbellam@buffalo.edu
 * Jagadeesh Vallabhaneni-----jvallabh@buffalo.edu
 * Saket Adusumilli-----------saketadu@buffalo.edu
 * Anil Nalamalapu------------anilkuma@buffalo.edu
 */
public class IndexScanOperator implements Operator {
	BufferedReader input;
	File f;
	ColumnInfo[] schema;
	ArrayList<Expression> conditions = new ArrayList<Expression>();
	Evaluator eval;
	Iterator<Expression> iterator;
	Expression currCondition;
	Expression indexCondition;
	int indexType;
	static RecordManager indexFile;
	BTree tree;
	int index;
	TupleBrowser browser;
	boolean exitCondition;
	Iterator<Datum[]> iter;
	jdbm.helper.Tuple tuple = new jdbm.helper.Tuple();
	
	public IndexScanOperator(File f, ColumnInfo[] schema) {
		this.f = f;
		this.schema = schema;
		reset(); 
		this.eval = new Evaluator(schema);
	}
	
	public void processIndexScan() {
		indexCondition = getIndexCondition();
		if(indexType!=-1){
			try {
				tree = BTree.load(indexFile,indexFile.getNamedObject(schema[index].origTableName+"_"+ schema[index].colDef.getColumnName()));
				if(indexType == 0){
					((EqualsTo)indexCondition).getRightExpression().accept(eval);
					browser = tree.browse(new Datum(eval.getValue()));
				}
				else if(indexType == 1){
					((MinorThan)indexCondition).getRightExpression().accept(eval);
					browser = tree.browse(new Datum(eval.getValue()));
				}
				else if(indexType == 2){
					((MinorThanEquals)indexCondition).getRightExpression().accept(eval);
					browser = tree.browse(new Datum(eval.getValue()));
					ArrayList<Datum[]> tupleList= (ArrayList<Datum[]>) tree.find(new Datum(eval.getValue()));
					browser.getNext(tuple);
				}
				else if(indexType == 3){
					((GreaterThan)indexCondition).getRightExpression().accept(eval);
					browser = tree.browse(new Datum(eval.getValue()));
					browser.getNext(tuple);
				}
				else{
					((GreaterThanEquals)indexCondition).getRightExpression().accept(eval);
					browser = tree.browse(new Datum(eval.getValue()));
				}
					
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	@Override
	public Datum[] readOneTuple() {
		Datum[] tuple=null;
		if(indexType!=-1){
			while(true) {
			try{
				if (indexType == 1||indexType == 2)
					tuple = lessThanTuple();
				else if (indexType == 3 || indexType == 4)
					tuple = greaterThanTuple();
				else 
					tuple = equalTuple();
			}
			catch(Exception e){
				e.printStackTrace();
			}
			if(tuple == null) return null;
			if(evaluateTuple(tuple))
				break;
			}
			return tuple;
		} else {
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

	public Expression getIndexCondition() {
		indexType = -1;
		if(conditions.size() == 0) {
			return null;
		} else {
			iterator = conditions.iterator();
			while (iterator.hasNext()) {
				Expression currExp = iterator.next();
				if (currExp instanceof EqualsTo) {
					indexType = 0;
					index = eval.getColumnID((Column) ((EqualsTo)currExp).getLeftExpression());
							
					return currExp;
				}
			}
			iterator = conditions.iterator();
			while (iterator.hasNext()) {
				Expression currExp = iterator.next();
				if (currExp instanceof MinorThan) {
					indexType = 1;
					index = eval.getColumnID((Column) ((MinorThan)currExp).getLeftExpression());
					iterator.remove();
					return currExp;					
				} else if (currExp instanceof MinorThanEquals) {
					indexType = 2;
					index = eval.getColumnID((Column) ((MinorThanEquals)currExp).getLeftExpression());
					iterator.remove();
					return currExp;					
				} else if (currExp instanceof GreaterThan) {
					indexType = 3;
					index = eval.getColumnID((Column) ((GreaterThan)currExp).getLeftExpression());
					iterator.remove();
					return currExp;					
				} else if (currExp instanceof GreaterThanEquals) {
					indexType = 4;
					index = eval.getColumnID((Column) ((GreaterThanEquals)currExp).getLeftExpression());
					iterator.remove();
					return currExp;
				}
			}
			return null;
		}
	}
	
	@Override
	public ColumnInfo[] getSchema() {
		return schema;
	}

	private boolean evaluateTuple(Datum[] tuple){
		boolean result = true;
		if(conditions.size() != 0){
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
	
	public Datum[] lessThanTuple() throws IOException{
		while(!exitCondition){
			if(iter==null){
				if(browser.getPrevious(tuple)){
					ArrayList<Datum[]> tupleList = (ArrayList<Datum[]>) tuple.getValue();
					iter = tupleList.iterator();
					while(iter.hasNext()) {
						Object[] obj = iter.next();
						Datum[] tuples = Arrays.asList(obj).toArray(new Datum[obj.length]);
						return tuples;
					}
					iter=null;
				}
				else {
					exitCondition = true;
				}
			}
			else {
				while(iter.hasNext()) {
					Object[] obj = iter.next();
					Datum[] tuples = Arrays.asList(obj).toArray(new Datum[obj.length]);
					return tuples;
				}
				iter=null;
			}
		}
		return null;
	}
	
	public Datum[] greaterThanTuple() throws IOException{
		while(!exitCondition){
			if(iter==null){
				if(browser.getNext(tuple)){
					ArrayList<Datum[]> tupleList = (ArrayList<Datum[]>) tuple.getValue();
					iter = tupleList.iterator();
					while(iter.hasNext()) {
						Object[] obj = iter.next();
						Datum[] tuples = Arrays.asList(obj).toArray(new Datum[obj.length]);
						return tuples;
					}
					iter=null;
				}
				else {
					exitCondition = true;
				}
			}
			else {
				while(iter.hasNext()) {
					Object[] obj = iter.next();
					Datum[] tuples = Arrays.asList(obj).toArray(new Datum[obj.length]);
					return tuples;
				}
				iter=null;
			}
		}
		return null;
	}
	
	public Datum[] equalTuple() throws IOException{
		while(!exitCondition){
			if(iter==null){
				if(browser.getNext(tuple)){
					ArrayList<Datum[]> tupleList = (ArrayList<Datum[]>) tuple.getValue();
					iter = tupleList.iterator();
					while(iter.hasNext()) {
						Object[] obj = iter.next();
						Datum[] tuples = Arrays.asList(obj).toArray(new Datum[obj.length]);
						return tuples;
					}
					iter=null;
					exitCondition = true;
				}
				else {
					exitCondition = true;
				}
			}
			else {
				while(iter.hasNext()) {
					Object[] obj = iter.next();
					Datum[] tuples = Arrays.asList(obj).toArray(new Datum[obj.length]);
					return tuples;
				}
				exitCondition = true;
			}
		}
		return null;
	}
}
