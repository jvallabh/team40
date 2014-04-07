package edu.buffalo.cse562;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.util.ArrayList;

import sun.misc.Hashing;

import net.sf.jsqlparser.expression.Expression;

public class ExternalHashOperator implements Operator{
	Operator input1;
	Operator input2;
	int index1, index2;
	ColumnInfo[] schema;
	public static final int hashSize = 8;
	public static final String tmpdirectory = Main.swapDir;
	File[] file1 = new File[hashSize];
	File[] file2 = new File[hashSize];
    OutputStream[] out1 = new OutputStream[hashSize];
    BufferedWriter[] fbw1 = new BufferedWriter[hashSize]; 
    OutputStream[] out2 = new OutputStream[hashSize];
    BufferedWriter[] fbw2 = new BufferedWriter[hashSize]; 
	HashJoin hashJoin;
	ArrayList<Expression> whereJoinCondition;
	ArrayList<Integer[]> whereJoinIndexes;
	int currentIter = 0;
	
	
	public ExternalHashOperator(Operator input1, Operator input2) {
		this.input1 = input1;
		this.input2 = input2;
		this.schema = getSchema(input1.getSchema(), input2.getSchema());
		try {
			initializeBuffers();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void buildHash() {
		index1 = whereJoinIndexes.get(0)[0];
		index2 = whereJoinIndexes.get(0)[1];
		try {
			getHashedBuckets(input1, index1, fbw1);
			getHashedBuckets(input2, index2, fbw2);
		} catch (IOException e) {
			e.printStackTrace();
		}
		createHashJoin();
	}
	
	public void initializeBuffers() throws IOException {
		for (int i=0; i<hashSize; i++) {
			file1[i] = File.createTempFile("hashBucket",".txt", new File(tmpdirectory));
			file2[i] = File.createTempFile("hashBucket",".txt", new File(tmpdirectory));
			file1[i].deleteOnExit();
			file2[i].deleteOnExit();
			out1[i] = new FileOutputStream(file1[i]);
			out2[i] = new FileOutputStream(file2[i]);
	        fbw1[i] = new BufferedWriter(new OutputStreamWriter(out1[i], Charset.defaultCharset()), 1024*1024);
	        fbw2[i] = new BufferedWriter(new OutputStreamWriter(out2[i], Charset.defaultCharset()), 1024*1024);
		}
	}
	
	public void getHashedBuckets(Operator input, int index, BufferedWriter[] fbw) throws IOException {
		Datum[] tuple = input.readOneTuple();
		while (tuple != null) {
			fbw[java.lang.Math.abs(tuple[index].String().hashCode()%hashSize)].write(toDatumString(tuple));
			fbw[java.lang.Math.abs(tuple[index].String().hashCode()%hashSize)].newLine();
			tuple = input.readOneTuple();
		}
		for (int i=0; i<hashSize; i++) {
			fbw[i].close();
		}
	}

	public String toDatumString(Datum[] tuple) {
		StringBuilder datumToString = new StringBuilder();
		for(Datum d:tuple){
			datumToString.append("|"+d.toString());
		}
		return datumToString.substring(1);
	}
	
	public void createHashJoin() {
		if (currentIter >= hashSize) {
			hashJoin = null;
		} else {
			ScanOperator scanOp1 = new ScanOperator(file1[currentIter], input1.getSchema());
			ScanOperator scanOp2 = new ScanOperator(file2[currentIter], input2.getSchema());
			hashJoin = new HashJoin(scanOp1, scanOp2, null);
			hashJoin.whereJoinCondition = whereJoinCondition;
			hashJoin.whereJoinIndexes = whereJoinIndexes;
			hashJoin.buildHash();
			currentIter++;
		}
	}
    
	public ColumnInfo[] getSchema(ColumnInfo[] schema1, ColumnInfo[] schema2){
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
	public Datum[] readOneTuple() {
		Datum[] tup = null;
		while(tup == null) {	
			if(hashJoin == null)
				return null;
			tup =  hashJoin.readOneTuple();
			if(tup == null) {
				createHashJoin();
				continue;
			}    
		}	
		return tup;		
	}
	
	@Override
	public void reset() {
		// TODO Auto-generated method stub
	}

	@Override
	public ColumnInfo[] getSchema() {
		return schema;
	}
}
