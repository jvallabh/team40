/**
 * 
 */
package edu.buffalo.cse562;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import net.sf.jsqlparser.statement.create.table.ColumnDefinition;

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
	ColumnDefinition[] schema;
	
	public ScanOperator(File f, ColumnDefinition[] schema) {
		this.f = f;
		this.schema = schema;
		reset();
	}
	
	@Override
	public Datum[] readOneTuple() {
		if (input == null) return null;
		String line = null;
		try {
			line = input.readLine();
		} catch(IOException e) {
			e.printStackTrace();
		}
		if (line == null) return null;
		String[] cols = line.split("\\|");
		Datum[] ret = new Datum[cols.length];
		for (int i=0; i<cols.length; i++) {
			//ret[i] = new Datum.Long(cols[i]);
			ret[i] = new Datum(cols[i]);
		}
		return ret;
	}


	@Override
	public void reset() {
		try {
			input = new BufferedReader(new FileReader(f));
		} catch (IOException e) {
			e.printStackTrace();
			input = null;
		}
	}

	@Override
	public ColumnDefinition[] getSchema() {
		// TODO Auto-generated method stub
		return schema;
	}
}
