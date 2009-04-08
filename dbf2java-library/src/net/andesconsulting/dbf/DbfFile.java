package net.andesconsulting.dbf;

import java.io.File;
import java.io.FileNotFoundException;

/**
 * Manejador de archivos DBF.<br/> Posee métodos que son análogos a los
 * comandos de Foxpro.
 *
 * @author Diego Enrique Silva Límaco (dsilva@andesconsulting.net)
 *
 */
public class DbfFile extends com.diesiljava.dbf.DbfFile {

	public DbfFile(File file) throws FileNotFoundException {
		super(file);
	}

	public DbfFile(String nombre) throws FileNotFoundException {
		super(nombre);
	}

}
