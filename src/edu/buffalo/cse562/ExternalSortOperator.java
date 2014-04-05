package edu.buffalo.cse562;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.OrderByElement;

public class ExternalSortOperator implements Operator{
	Operator input;
	List<OrderByElement> orderByColumns;
	int[] orderIndex;
	ColumnInfo[] schema;
	ScanOperator scanOperator;
	int index;
	int[] orderByColumnIndex;
	
	public ExternalSortOperator(Operator input, List<OrderByElement> orderByColumns, String tmpdirectory,int index) {
		this.input = input;		
		this.orderByColumns = orderByColumns;
		this.schema = input.getSchema();
		if(index==-1){
			getOrderByColumnIndexes();
		}
		else{
			orderByColumnIndex=new int[]{index};
		}			
		this.index = index;
		this.scanOperator = new ScanOperator(mergeFiles(sortBlock(input,tmpdirectory),tmpdirectory),schema);
		scanOperator.conditions = new ArrayList<>();
	}

	public List sortBlock(Operator operator, String tmpdirectory) {
		if(index==-1) {
		Datum[] currTuple;
		List<File> files =  new ArrayList<File>();
		long count = 10000;
		SortableTuple.schema=schema;
		SortableTuple.orderByColumnIndex=orderByColumnIndex;
		SortableTuple.orderIndex=orderIndex;
		ArrayList<SortableTuple> sortableTuples = new ArrayList<SortableTuple>();
		while((currTuple = readOneTupleFromInput()) != null) {
			SortableTuple sortableTuple = new SortableTuple(currTuple);
			sortableTuples.add(sortableTuple);
			if(count>0){
			count--;
			}
			else {
			Collections.sort(sortableTuples, new SortableTuple(null));
			files.add(saveBlock(sortableTuples,tmpdirectory));
			sortableTuples = new ArrayList<>();
			count = 10000;
			}
		}
		Collections.sort(sortableTuples, new SortableTuple(null));
		files.add(saveBlock(sortableTuples,tmpdirectory));
		return files;
		}
		else {
			Datum[] currTuple;
			long count = 10000;
			List<File> files =  new ArrayList<File>();
			SortableTuple.schema=schema;
			SortableTuple.orderByColumnIndex=orderByColumnIndex;
			//SortableTuple.orderByColumnIndex=new int[]{index};
			SortableTuple.orderIndex=new int[]{1};
			ArrayList<SortableTuple> sortableTuples = new ArrayList<SortableTuple>();
			while((currTuple = readOneTupleFromInput()) != null) {
				SortableTuple sortableTuple = new SortableTuple(currTuple);
				sortableTuples.add(sortableTuple);
				if(count>0){
				count--;
				}
				else {
				Collections.sort(sortableTuples, new SortableTuple(null));
				files.add(saveBlock(sortableTuples,tmpdirectory));
				sortableTuples = new ArrayList<>();
				count = 10000;
				}
			}
			Collections.sort(sortableTuples, new SortableTuple(null));
			files.add(saveBlock(sortableTuples,tmpdirectory));
			return files;
		}
	}
	
	public File saveBlock(ArrayList<SortableTuple> sortableTuples, String tmpdirectory) {
		File newtmpfile = null;
		try{
			newtmpfile = File.createTempFile("sortedBlock",".txt", new File(tmpdirectory));	
	        OutputStream out = new FileOutputStream(newtmpfile);
	        BufferedWriter fbw = new BufferedWriter(new OutputStreamWriter(
	                out, Charset.defaultCharset()));
			for(SortableTuple curr:sortableTuples){
				fbw.write(toDatumString(curr.tuple));
				fbw.newLine();
			}
			fbw.close();
		}
		catch(Exception e){
			e.printStackTrace();
		}
		return newtmpfile;
	}

	private Datum[] readOneTupleFromInput() {
		return input.readOneTuple();
	}
    
	public File mergeFiles(List<File> files, String tmpdirectory) {
		List<File> fileList = files;
		ScanOperator scan1,scan2;
		File file1,file2;
		SortableTuple sortHelper = new SortableTuple(null);
		Datum[] d1,d2;
		File newtmpfile = null;
		while(files.size()!=1) {
			try{
				newtmpfile = File.createTempFile("sortedBlock",".txt", new File(tmpdirectory));
		        OutputStream out = new FileOutputStream(newtmpfile);
		        BufferedWriter fbw = new BufferedWriter(new OutputStreamWriter(out, Charset.defaultCharset()));
				file1 = files.get(0);
				file2 = files.get(1);
				scan1 = new ScanOperator(file1, schema);
				scan1.conditions = new ArrayList<>();
				scan2 = new ScanOperator(file2, schema);
				scan2.conditions = new ArrayList<>();
				d1=scan1.readOneTuple();
				d2=scan2.readOneTuple();
				while(d1!=null&&d2!=null) {
					if(sortHelper.compare(new SortableTuple(d1), new SortableTuple(d2))<0) {
						fbw.write(toDatumString(d1));
						fbw.newLine();
						d1=scan1.readOneTuple();
					}
					else{
						fbw.write(toDatumString(d2));
						fbw.newLine();
						d2=scan2.readOneTuple();
					}
				}
				if(d1==null){
					while(d2!=null){
					fbw.append(toDatumString(d2));
					fbw.newLine();
					d2=scan2.readOneTuple();
					}
				}
				else {
					while(d1!=null){
					fbw.append(toDatumString(d1));
					fbw.newLine();
					d1=scan1.readOneTuple();
					}
				}	
				fbw.close();
				fileList.add(newtmpfile);
				file1.delete();
				file2.delete();
				fileList.remove(0);
				fileList.remove(0);
			}
			catch(Exception e){
				e.printStackTrace();
			}
		}
		files.get(0).deleteOnExit();
		return files.get(0);
	}
	
	public String toDatumString(Datum[] tuple) {
		StringBuilder datumToString = new StringBuilder();
		for(Datum d:tuple){
			datumToString.append("|"+d.toString());
		}
		return datumToString.substring(1);
	}
    
    public Datum[] readOneTuple() {
    	return scanOperator.readOneTuple();
    }
    
    private void getOrderByColumnIndexes() {
		orderByColumnIndex = new int[orderByColumns.size()];
		orderIndex = new int[orderByColumns.size()];
		for(int i=0;i<orderByColumns.size();i++){
			if(orderByColumns.get(i).isAsc()){
				orderIndex[i]=1;
			}
			else{
				orderIndex[i]=-1;
			}
			Column currColumn = (Column)orderByColumns.get(i).getExpression();
			String currColumnName = currColumn.getColumnName();
			String currColumnTable = currColumn.getTable().getName();
			for(int j=0;j<schema.length;j++){
				boolean columnMatch = false;
				if(currColumnTable != null){
					columnMatch = schema[j].tableName.equalsIgnoreCase(currColumnTable) && schema[j].colDef.getColumnName().equalsIgnoreCase(currColumnName);
				}
				else{
					columnMatch = schema[j].colDef.getColumnName().equalsIgnoreCase(currColumnName);
				}
				if(columnMatch){
					orderByColumnIndex[i]=j;
					break;
				}
			}
		}		
	}

	@Override
	public void reset() {

	}

	@Override
	public ColumnInfo[] getSchema() {
		return schema;
	}

}
