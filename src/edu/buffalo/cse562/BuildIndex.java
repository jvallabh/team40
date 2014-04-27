package edu.buffalo.cse562;


import java.io.Serializable;
import java.util.Comparator;

import jdbm.*;
import jdbm.btree.BTree;

public class BuildIndex {
	BTree tree;
	Operator input;
	int index;
	ColumnInfo[] schema;
	RecordManager indexFile;
	
	public BuildIndex(Operator input,int index) {
		this.input = input;
		this.index = index;
	}
	
	public void buildIndex() throws Exception{
		this.schema = input.getSchema();
		long recid;
		indexFile = RecordManagerFactory.createRecordManager(Main.indexDir+"Index");
		recid = indexFile.getNamedObject(schema[0].tableName+"_"+schema[0].colDef.getColumnName());
		if(recid != 0) {
			tree = BTree.load(indexFile, recid);
		}
		else {
			tree = BTree.createInstance(indexFile, new ExampleComparator());
			indexFile.setNamedObject(schema[0].tableName+"_"+schema[0].colDef.getColumnName(), tree.getRecid());
		}
		Datum[] tuple = input.readOneTuple();
		int i=1000;
		while(tuple!=null) {
			tree.insert(tuple[0], tuple, false);
			tuple = input.readOneTuple();
			if(i==0){
				i=1000;
				indexFile.commit();
			}
			else
				i--;
		}
		indexFile.close();
	}
	
	public static class ExampleComparator  implements Serializable,Comparator<Datum>{
    	
	  	  public int compare(Datum obj1, Datum obj2) {
	  	    return obj1.Int().compareTo(obj2.Int());
	  	  }
	  	}


}
