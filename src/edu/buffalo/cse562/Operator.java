/**
 * 
 */
package edu.buffalo.cse562;


/**
 * @author The Usual Suspects
 * @Name----------------------EmailAddress
 * Praveen Bellam-------------pbellam@buffalo.edu
 * Jagadeesh Vallabhaneni-----jvallabh@buffalo.edu
 * Saket Adusumilli-----------saketadu@buffalo.edu
 * Anil Nalamalapu------------anilkuma@buffalo.edu
 */
public interface Operator {
	
	public Datum[] readOneTuple();
	
	public void reset();
	
	public ColumnInfo[] getSchema();
}
