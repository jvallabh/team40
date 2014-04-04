/**
 * 
 */
package edu.buffalo.cse562;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.schema.Column;
/**
 * @author The Usual Suspects
 * @Name----------------------EmailAddress
 * Praveen Bellam-------------pbellam@buffalo.edu
 * Jagadeesh Vallabhaneni-----jvallabh@buffalo.edu
 * Saket Adusumilli-----------saketadu@buffalo.edu
 * Anil Nalamalapu------------anilkuma@buffalo.edu
 */
public class AggrOperator implements Operator {
	Operator input;
	ColumnInfo[] schema;
	List<SelectItem> selectItems;
	ArrayList<Integer> itemList = new ArrayList<>();
	Datum[] result=null;
	int[] selectItemType;
	int resultCount =0;
	int tmpCount=0;
	int distCount = 0;
	String prevDatum = null;
	private static int sum=1,avg=2,count=3,min=4,max=5,distinct=6;
	public AggrOperator(Operator input, ColumnInfo[] schema, List<SelectItem> selectItems) {
		this.input = input;
		this.schema = schema;
		this.selectItems = selectItems;
		this.selectItemType= getSelectItemType(schema);
		this.result = input.readOneTuple();
		tmpCount++;
	}
	public int getColumnID(Column col) {
		for (int i=0; i<schema.length; i++) {
			if (schema[i].colDef.getColumnName().equals(col.getColumnName())) {
				return i;
			}
		}
		return -1;
	}
	@Override
	public Datum[] readOneTuple() {
		while(result!=null) {
			Datum[] tuple = input.readOneTuple();
			if(tuple==null) {
				Datum[] tmp = result;
				result =null;
				resultCount =tmpCount;
				return getResult(tmp);
			}
			tmpCount++;
			for (int i=0; i<tuple.length;i++) {
				if(selectItemType[i]==0 && !(result[i].element.equals(tuple[i].element))) {
					Datum [] tmp = result;
					result =tuple;
					resultCount= tmpCount-1;
					tmpCount =1;
					return getResult(tmp);
				}
			}
			getUpdate(tuple);
		}
		return getResult(result);
	}
	public Datum[] getResult(Datum[] tuple) {
		if(tuple==null)
		return null;
		for (int i=0; i<tuple.length;i++) {
			if(selectItemType[i]==avg)
			tuple[i].element = Double.toString((Double.parseDouble(tuple[i].toString())*1000.0) /(resultCount*1000.0));
			if(selectItemType[i]==distinct){
				prevDatum = null;
				tuple[i].element = new String(Integer.toString(distCount));
				distCount = 0;
			}
		}
		
		return tuple;
	}
	public void getUpdate(Datum[] tuple) {
		for (int i=0; i<tuple.length;i++) {
			if(selectItemType[i]==sum)
			result[i].element = Math.getSum(result[i].toString(), tuple[i].toString(),i,schema); 
			else if(selectItemType[i]==min)
			result[i].element = Math.getMin(result[i].toString(), tuple[i].toString(),i,schema); 
			else if(selectItemType[i]==max)
			result[i].element = Math.getMax(result[i].toString(), tuple[i].toString(),i,schema); 
			else if(selectItemType[i]==avg)
			result[i].element = Math.getSum(result[i].toString(), tuple[i].toString(),i,schema); 
			else if(selectItemType[i]==count)
			result[i].element = Math.getSum(result[i].toString(), tuple[i].toString(),i,schema);
			else if(selectItemType[i]==distinct)
			{   
				if(prevDatum == null)
				{
				distCount +=1;
				}
				else if(!prevDatum.contentEquals(tuple[i].toString()))
				{
					distCount +=1;
				}
				prevDatum = new String( tuple[i].toString());
			}
			
		}
	}
	@Override
	public void reset() {
		input.reset();
	}
	public static int[] getSelectItemType( ColumnInfo[] schema ) {
		int[] selectItemType = new int[schema.length];
		int i=0;
		for (ColumnInfo a: schema) {
			selectItemType[i++]=a.functionType;
		}
		return selectItemType;
	}
	@Override
	public ColumnInfo[] getSchema() {
		// TODO Auto-generated method stub
		return schema;
	}
}