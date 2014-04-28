package edu.buffalo.cse562;


import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;

import jdbm.*;
import jdbm.btree.BTree;

public class BuildIndex {
	BTree tree;
	Operator input;
	int index;
	ColumnInfo[] schema;
	RecordManager indexFile;
	HashMap<String,int[]> tableIndex = new HashMap<String,int[]>();
	
	
	public BuildIndex(Operator input,int index) {
		this.input = input;
		this.index = index;
		try{
			this.indexFile = RecordManagerFactory.createRecordManager(Main.indexDir+"Index");
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	
	public void buildTableIndex(){
		tableIndex.put("lineitem", new int[]{0,2});
		tableIndex.put("orders", new int[]{0,1});
		tableIndex.put("customer", new int[]{0,3});
		tableIndex.put("supplier", new int[]{0,3});
		tableIndex.put("nation", new int[]{0,2});
	}
	
	public void buildIndex() throws Exception{
		this.schema = input.getSchema();
		long recid;
		String type;
		for(int col:tableIndex.get(this.schema[0].tableName.toLowerCase())) {
		type = this.schema[col].colDef.getColDataType().toString();
		recid = indexFile.getNamedObject(this.schema[col].tableName+"_"+this.schema[col].colDef.getColumnName());
		if(recid != 0) {
			tree = BTree.load(indexFile, recid);
		}
		else {
			tree = BTree.createInstance(indexFile, new ExampleComparator(type));
			indexFile.setNamedObject(this.schema[col].tableName+"_"+this.schema[col].colDef.getColumnName(), tree.getRecid());
		}
		Datum[] tuple = input.readOneTuple();
		int i=1000;
		ArrayList<Datum[]> tupleList;
		while(tuple!=null) {
			tupleList = (ArrayList<Datum[]>)tree.find(tuple[col]);
			if(tupleList==null)
				tupleList = new ArrayList<>();
			tupleList.add(tuple);
			tree.insert(tuple[col], tupleList, false);
			tuple = input.readOneTuple();
			if(i==0){
				i=1000;
				indexFile.commit();
			}
			else
				i--;
		}
		input.reset();
		System.out.println(tree.size());
		}
	}
	
	public static class ExampleComparator  implements Serializable,Comparator<Datum>{
		String type;
		int x;
		
		public ExampleComparator(String type) {
			this.type = type;
		}
		
    	
  	  public int compare(Datum obj1, Datum obj2) {
  		  
  		  if(type.equalsIgnoreCase("int"))
  			 x = Integer.compare(Integer.parseInt(obj1.element),Integer.parseInt(obj2.element));
  		  else if(type.equalsIgnoreCase("DECIMAL"))
  			x = Double.compare(Double.parseDouble(obj1.element),Double.parseDouble(obj2.element));
  		else if(type.equalsIgnoreCase("double"))
  			x = Double.compare(Double.parseDouble(obj1.element),Double.parseDouble(obj2.element));
  		else if(type.equalsIgnoreCase("string")||type.equalsIgnoreCase("VARCHAR")||type.equalsIgnoreCase("CHAR"))
  			x = obj1.toString().compareTo(obj2.toString());
  		else if(type.equalsIgnoreCase("DATE"))
  			x = obj1.Date().compareTo(obj2.Date());
  		  return x;
  	  }
	  	  
	  	  
	  	}


}
