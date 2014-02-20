/**
 * 
 */
package edu.buffalo.cse562;

import net.sf.jsqlparser.statement.create.table.ColumnDefinition;

/**
 * @author The Usual Suspects
 * @Name----------------------EmailAddress
 * Praveen Bellam-------------pbellam@buffalo.edu
 * Jagadeesh Vallabhaneni-----jvallabh@buffalo.edu
 * Saket Adusumilli-----------saketadu@buffalo.edu
 * Anil Nalamalapu------------anilkuma@buffalo.edu
 */
public class ColumnInfo {
	ColumnDefinition colDef;
	String tableName;
	ColumnInfo(ColumnDefinition colDef, String tableName){
		this.colDef=colDef;
		this.tableName=tableName;
	}

}
