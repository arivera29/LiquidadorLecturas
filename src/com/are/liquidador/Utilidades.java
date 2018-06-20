package com.are.liquidador;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Utilidades {
	
	public static FilenameFilter FilterFileLess452Comprimido() {
		return new FilenameFilter() {

			@Override
			public boolean accept(File dir, String name) {
				if (name.lastIndexOf('.') > 0) {
					// get last index for '.' char
					int lastIndex = name.lastIndexOf('.');

					// get extension
					String str = name.substring(lastIndex);

					// match path name extension
					if (str.equals(".Z") && name.contains("lese0452")) {
						return true;
					}
				}
				return false;
			}
		};
	}

	public static FilenameFilter FilterFileLess451Comprimido() {
		return new FilenameFilter() {

			@Override
			public boolean accept(File dir, String name) {
				if (name.lastIndexOf('.') > 0) {
					// get last index for '.' char
					int lastIndex = name.lastIndexOf('.');

					// get extension
					String str = name.substring(lastIndex);

					// match path name extension
					if (str.equals(".Z") && name.contains("lese0451")) {
						return true;
					}
				}
				return false;
			}
		};
	}
	
	public static FilenameFilter FilterFileLessNoComprimido(String name) {
		return new FilenameFilter() {

			@Override
			public boolean accept(File dir, String name) {
				if (name.contains(name)) {
					File f = new File(name);
					if (!getFileExtension(f).equals("Z")) {
						return true;
					}
				}
				return false;
			}
		};
	}
	
	public static FilenameFilter FilterFileLess350Comprimido() {
		return new FilenameFilter() {

			@Override
			public boolean accept(File dir, String name) {
				if (name.lastIndexOf('.') > 0) {
					// get last index for '.' char
					int lastIndex = name.lastIndexOf('.');

					// get extension
					String str = name.substring(lastIndex);

					// match path name extension
					if (str.equals(".Z") && name.contains("lese0350")) {
						return true;
					}
				}
				return false;
			}
		};
	}
	
	
	public static FilenameFilter FilterFileLess355Comprimido() {
		return new FilenameFilter() {
			@Override
			public boolean accept(File dir, String name) {
				if (name.lastIndexOf('.') > 0) {
					// get last index for '.' char
					int lastIndex = name.lastIndexOf('.');

					// get extension
					String str = name.substring(lastIndex);

					// match path name extension
					if (str.equals(".Z") && name.contains("lese0355")) {
						return true;
					}
				}
				return false;
			}
		};
	}
	
	public static boolean DescomprimirArchivo(String pathOrigen, String dirDestino) {
		boolean result = false;
		String comando = "C:\\Program Files\\7-Zip\\7z.exe X -yo"
				+ dirDestino + " " + pathOrigen;
		String salida = null;
		boolean descomprimido = false;
		System.out.println("Comando: " + comando);
		AgregarLog("Comando: " + comando);
		try {

			Process proceso = Runtime.getRuntime().exec(comando);
			InputStreamReader entrada = new InputStreamReader(proceso.getInputStream());
			BufferedReader stdInput = new BufferedReader(entrada);

			// Si el comando tiene una salida la mostramos
			if ((salida = stdInput.readLine()) != null) {
				System.out.println("Comando ejecutado Correctamente");
				AgregarLog("Comando ejecutado Correctamente");
				while ((salida = stdInput.readLine()) != null) {
					System.out.println(salida);
					AgregarLog(salida);
					if (salida.contains("Everything is Ok")) {
						descomprimido = true;
					}
				}

				if (descomprimido) {
					System.out.println("Archivo descomprimido");
					AgregarLog("Archivo descomprimido");
					/*File file = new File(pathOrigen);
					if (file.exists()) {
						//file.delete();
						System.out.println("Archivo comprimido Eliminado "+ file.getPath());
						AgregarLog("Archivo comprimido Eliminado "+ file.getPath());
					}
					*/
					result = true;
				}

			} else {
				System.out.println("No se ha producido ninguna salida");
				AgregarLog("No se ha producido ninguna salida");
			}
			
			entrada.close();

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		
		
		return result;
	}
	
	public static void AgregarLog(String log) {
		FileWriter fichero = null;
		java.util.Date f1 = new Date();
		DateFormat df = new SimpleDateFormat("yyyyMMdd");
		String fileLog = df.format(f1) + ".txt";
		try {
			fichero = new FileWriter("C:\\LECTURAS\\" + fileLog,true);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		if (fichero != null) {
			PrintWriter pw = null;
	        try
	        {
	        	String tab = "	";
	            pw = new PrintWriter(fichero);
	            java.util.Date fecha = new Date();
	    		df = new SimpleDateFormat("yyyy-MM-dd HH:mm");
	    		pw.append(df.format(fecha) + tab + log + "\r\n" );
	    		fichero.close();
	
	        } catch (Exception e) {
	            e.printStackTrace();
	        }
	        
		}
	}
	
	public static void moverFicherosNoComprimidos(String dirOrigen, String dirDestino, String filterFile) {
		Utilidades.AgregarLog("Moviendo archivos no comprimidos " + filterFile );
		File fout = new File(dirOrigen);
		File dir = new File(dirDestino);
		if (!dir.exists()) {
			dir.mkdirs();
		}
		File[] ficheros = fout.listFiles(); // Leer archivos
		
		for (int x = 0; x < ficheros.length; x++) {
			String ruta = ficheros[x].getPath();
			File f = new File(ruta);
			if (f.getName().contains(filterFile) && !getFileExtension(f).equals("Z")) {
				String nuevo = dirDestino + "\\" + f.getName();
				f.renameTo(new File(nuevo));
				Utilidades.AgregarLog("Moviendo archivo: " + ruta + " a " + nuevo);
			}
		}
		
		//Utilidades.AgregarLog("Archivos encontrados: " + ficheros.length);
	}

	
	public static void moverFichero(String path, String dirDestino) {
		
		File file = new File(path);
		if (file.exists()) {
			File dir = new File(dirDestino);
			if (!dir.exists()) {
				dir.mkdirs();
			}
			
			file.renameTo(new File(dirDestino + "\\" + file.getName()));
		}
		
	}
	
	public static String getFileExtension(File file) {
	    String name = file.getName();
	    try {
	        return name.substring(name.lastIndexOf(".") + 1);
	    } catch (Exception e) {
	        return "";
	    }
	}

}
