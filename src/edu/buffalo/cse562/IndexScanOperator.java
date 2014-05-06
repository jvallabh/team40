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
import java.util.List;
import java.util.Set;
import java.util.SortedMap;

import jdbm.PrimaryTreeMap;
import jdbm.RecordManager;
import jdbm.RecordManagerFactory;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.Between;
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
	int indexType=-1;
	RecordManager indexFile;
	PrimaryTreeMap<String, ArrayList<String>> tree;
	SortedMap<String,ArrayList<String>> betweenMap;
	String[] iterList;
	int index;
	boolean exitCondition;
	Iterator<String> iter;
	int where;
	int to;
	String whereKey, toKey;
	boolean hasBetween;
	int betweenWhere = 0;
	static RecordManager indexFile1=null;
	
	public IndexScanOperator(File f, ColumnInfo[] schema) {
		this.f = f;
		this.schema = schema;
		reset(); 
		this.eval = new Evaluator(schema);
		try{
			if(Main.tpch&&!Main.build)
			{
				if(indexFile1==null)
					indexFile1 = RecordManagerFactory.createRecordManager(Main.indexDir+"/"+"howla");
				indexFile = indexFile1;
			}
			else if(schema[0].origTableName.equals("nation"))
				indexFile = Main.indexFile;
			else
			indexFile = RecordManagerFactory.createRecordManager(Main.indexDir+"/"+"Index"+schema[0].tableName);
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	
	private Expression getGreaterThanWhereCondition(int columnIndexFound){
		if(conditions.size() == 0) {
			return null;
		} else {
			iterator = conditions.iterator();
			while (iterator.hasNext()) {
				Expression currExp = iterator.next();
				if (currExp instanceof GreaterThanEquals && !(((GreaterThanEquals)currExp).getRightExpression() instanceof Column)) {
					int currColindex = eval.getColumnID((Column) ((GreaterThanEquals)currExp).getLeftExpression());
					if(columnIndexFound==currColindex){
						iterator.remove();
						return currExp;						
					}					
				}
			}
			return null;
		}
		
	}
	
	public void processIndexScan() {
		indexCondition = getIndexCondition();
		if(indexCondition != null){
			Expression firstIndexCondition = indexCondition;			
			String tableName = this.schema[index].origTableName;
			if(tableName==null)
				tableName = this.schema[index].tableName;
			where = getIndexInTree(index, indexType, firstIndexCondition);
			if(firstIndexCondition instanceof MinorThanEquals && !(((MinorThanEquals)firstIndexCondition).getRightExpression() instanceof Column)){				
				whereKey = iterList[where+1];
				int columnId = eval.getColumnID(((Column)((MinorThanEquals)indexCondition).getLeftExpression()));
				Expression secondIndexCondition = getGreaterThanWhereCondition(columnId);
				if(secondIndexCondition != null){
					hasBetween = true;
					((GreaterThanEquals)secondIndexCondition).getRightExpression().accept(eval);
					toKey = eval.getValue();
					betweenMap= tree.subMap(toKey, whereKey);
					iterList = new String[betweenMap.size()];
					betweenMap.keySet().toArray(iterList);
				}
			}
			else if(firstIndexCondition instanceof MinorThan && !(((MinorThan)firstIndexCondition).getRightExpression() instanceof Column)){
					whereKey = iterList[where];
					int columnId = eval.getColumnID(((Column)((MinorThan)indexCondition).getLeftExpression()));
					Expression secondIndexCondition = getGreaterThanWhereCondition(columnId);
					if(secondIndexCondition != null){
						hasBetween = true;
						((GreaterThanEquals)secondIndexCondition).getRightExpression().accept(eval);
						toKey = eval.getValue();
						betweenMap= tree.subMap(toKey, whereKey);
						iterList = new String[betweenMap.size()];
						betweenMap.keySet().toArray(iterList);
					}
				}
		}
		/*
		if(indexType!=-1){
			try {
				tree = indexFile.treeMap(this.schema[index].tableName+"_"+this.schema[index].colDef.getColumnName());
				iterList = new String[tree.size()];
				tree.keySet().toArray(iterList);
				if(indexType == 0){
					((EqualsTo)indexCondition).getRightExpression().accept(eval);
					where = new ArrayList(tree.keySet()).indexOf(eval.getValue());
				}
				else if(indexType == 1){
					((MinorThan)indexCondition).getRightExpression().accept(eval);
					where = searchKey(eval.getValue());
				}
				else if(indexType == 2){
					((MinorThanEquals)indexCondition).getRightExpression().accept(eval);
					where = searchKey(eval.getValue());
				}
				else if(indexType == 3){
					((GreaterThan)indexCondition).getRightExpression().accept(eval);
					where = searchKey(eval.getValue());
					where++;
				}
				else{
					((GreaterThanEquals)indexCondition).getRightExpression().accept(eval);
					where = searchKey(eval.getValue());
				}
					
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}*/
	}
	
	private int getIndexInTree(int schemaIndex, int indexType, Expression condition_){
		int indexInTree = -1;
		String tableName;
		try {
			if(Main.tpch&&!Main.build)
				tree = indexFile.treeMap("howla");
			else
			tree = indexFile.treeMap(this.schema[schemaIndex].tableName+"_"+this.schema[schemaIndex].colDef.getColumnName());
			Set<String> set = tree.keySet();
			iterList = set.toArray(new String[set.size()]);
			if(indexType == 0){
				((EqualsTo)condition_).getRightExpression().accept(eval);
				indexInTree = Arrays.asList(iterList).indexOf(eval.getValue());
			}
			else if(indexType == 1){
				((MinorThan)condition_).getRightExpression().accept(eval);
				indexInTree = searchKey(eval.getValue());
			}
			else if(indexType == 2){
				((MinorThanEquals)condition_).getRightExpression().accept(eval);
				indexInTree = searchKey(eval.getValue());
			}
			else if(indexType == 3){
				((GreaterThan)condition_).getRightExpression().accept(eval);
				indexInTree = searchKey(eval.getValue());
				indexInTree++;
			}
			else{
				((GreaterThanEquals)condition_).getRightExpression().accept(eval);
				indexInTree = searchKey(eval.getValue());
			}
				
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return indexInTree;
		
	}
	
	@Override
	public Datum[] readOneTuple() {
		Datum[] tuple=null;
		if(indexType!=-1){
			while(true) {
			try{
				if(hasBetween){
					tuple = betweenTuple();
				}
				else if (indexType == 1||indexType == 2)
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
			if(Main.tpch&&!Main.build){
				Datum[] modifiedTuple = new Datum[3];
				for(int i=0;i<3;i++){
					modifiedTuple[i] = tuple[i];
				}
				return modifiedTuple;
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
		if(Main.tpch&&!Main.build)
			return;
		try {
			if(input != null){
				input.close();
			}
			if(f==null)
				return;
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
					iterator.remove();		
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
		if(Main.tpch&&!Main.build){
		ColumnInfo[] colinfo = new ColumnInfo[3]; 
		for(int i=0;i<3;i++)
		colinfo[i] = schema[i];
		return colinfo;
		}
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
	
	public Datum[] betweenTuple() throws IOException{
		while(!exitCondition){
			if(iter==null){
				int i=0;
				ArrayList<String> tupleList =betweenMap.get(iterList[betweenWhere]);
				betweenWhere++;
				iter = tupleList.iterator();
				while(iter.hasNext()) {
					String obj = iter.next();
					Datum[] tuples = toDatum(obj);
					return tuples;
				}
			}
			else {
				while(iter.hasNext()) {
					String obj = iter.next();
					Datum[] tuples = toDatum(obj);
					return tuples;
				}
				iter=null;
			}
			if(betweenWhere==iterList.length)
				exitCondition = true;
		}
		return null;
	}
	
	public Datum[] lessThanTuple() throws IOException{
		while(!exitCondition){
			if(iter==null){
				int i=0;
				ArrayList<String> tupleList =tree.get(iterList[where]);
				where--;
				iter = tupleList.iterator();
				while(iter.hasNext()) {
					String obj = iter.next();
					Datum[] tuples = toDatum(obj);
					return tuples;
				}
			}
			else {
				while(iter.hasNext()) {
					String obj = iter.next();
					Datum[] tuples = toDatum(obj);
					return tuples;
				}
				iter=null;
			}
			if(where<0)
				exitCondition = true;
		}
		return null;
	}
	
	public Datum[] equalTuple() throws IOException{
		while(!exitCondition){
			if(iter==null){
				if(tree.containsKey(eval.getValue())){
					int i=0;
					ArrayList<String> tupleList = tree.get(eval.getValue());
					//where--;
					iter = tupleList.iterator();
					while(iter.hasNext()) {
						String obj = iter.next();
						Datum[] tuples = toDatum(obj);
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
					String obj = iter.next();
					Datum[] tuples = toDatum(obj);
					return tuples;
				}
				exitCondition = true;
			}
		}
		return null;
	}
	
	public Datum[] greaterThanTuple() throws IOException{
		while(!exitCondition){
			if(iter==null){
				int i=0;
				ArrayList<String> tupleList = tree.get(iterList[where]);
				where++;
				iter = tupleList.iterator();
				while(iter.hasNext()) {
					String obj = iter.next();
					Datum[] tuples = toDatum(obj);
					return tuples;
				}
			}
			else {
				while(iter.hasNext()) {
					String obj = iter.next();
					Datum[] tuples = toDatum(obj);
					return tuples;
				}
				iter=null;
			}
			if(where == iterList.length)
				exitCondition = true;
		}
		return null;
	}
	
	
	
	public int searchKey(String value){
		List<String> list = Arrays.asList(iterList);
		int i,start=0,end=list.size();
		i=(start+end)/2;
		int compare;
		while(i>start&&i<end){
			compare = value.compareTo(list.get(i));
			if(compare==0)
				return i;
			else if(compare<0)
				end = i;
			else 
				start = i;
			i=(start+end)/2;
			if(i==start||i==end)
				return i;
		}
		return i;
	}
	
	public Datum[] toDatum(String[] tuple){
		Datum[] d = new Datum[tuple.length];
		int i=0;
		for(String s: tuple){
			d[i] = new Datum(s);
			i++;
		}
		return d;
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
