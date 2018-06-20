/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.are.liquidador;

import java.sql.SQLException;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Aimer
 */
public class LiquidadorLecturas {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        java.util.Date fecha = new Date();
        String periodo = "";

        if (args.length == 0) {
            System.out.println("Falta parametro");
            String anio = Integer.toString(fecha.getYear());
            String mes = Integer.toString(fecha.getMonth());
            if (mes.length() == 1) {
                mes = "0" + mes;
            }
            periodo = anio + mes;
        } else {
            periodo = args[0];
        }

        // Validando periodo
        if (periodo.length() != 6) {
            System.out.println("Longitud parametro periodo debe ser 6. " + periodo);
            return;
        }

        try {
            int anio = Integer.parseInt(periodo.substring(0, 4));
            int mes = Integer.parseInt(periodo.substring(4, 6));
            if (mes <= 0 || mes > 12) {
                System.out.println("Valor del mes debe estar entre 1 y 12. Anio " + anio + " Mes " + mes);
                return;
            }
        } catch (Exception e) {
            System.out.println("Error. " + e.getMessage() + ". " + periodo);
            return;
        }

        db conexion = null;
        //AplicarPoliticaDirectos(periodo);
        
        try {

            System.out.println("Inicio: " + fecha.toString());
            System.out.println("Clasificar Suministros");
            Inicializar(periodo);
            Clasificar(periodo);
            Excepciones(periodo);
            Duplicados(periodo);

            conexion = new db();

            long total1 = 0;
            Utilidades.AgregarLog("Iniciando proceso de liquidacion");
            String sql = "select id,unicom, num_itim,ruta, clasificacion,tipologia,anomalia "
                    + " FROM lecturas "
                    + " WHERE REPLACE(CONVERT(VARCHAR(7), f_lect, 120), '-', '') = ?  AND NOT anomalia IS NULL AND duplicado=0 and excluido=0 ";

            sql += " ORDER BY unicom,ruta,num_itim,clasificacion,tipologia ";

            java.sql.PreparedStatement pst = conexion.getConnection().prepareStatement(sql);
            pst.setString(1, periodo);
            java.sql.ResultSet rs = conexion.Query(pst);

            while (rs.next()) {
                System.out.println("Liquidando: Id:" + rs.getLong("id") + " Unicom: " + rs.getString("unicom") + " Clasificacion:  " + rs.getString("clasificacion") + " Tipologia: " + rs.getString("tipologia"));
                //Lectura OK
                Utilidades.AgregarLog("Liquidando: Id:" + rs.getLong("id") + " Unicom: " + rs.getString("unicom") + " Clasificacion:  " + rs.getString("clasificacion") + " Tipologia: " + rs.getString("tipologia"));
                sql = "SELECT * FROM precio_contrato WHERE unicom =? and clasificacion =? and tipologia = ?";
                java.sql.PreparedStatement pst1 = conexion.getConnection().prepareStatement(sql);
                pst1.setString(1, rs.getString("unicom"));
                pst1.setString(2, rs.getString("clasificacion"));
                pst1.setString(3, rs.getString("tipologia"));

                java.sql.ResultSet rs1 = conexion.Query(pst1);
                if (rs1.next()) {
                    //Contrato encontrado
                    System.out.println("Contrato encontrado: " + rs1.getString("contrato"));
                    Utilidades.AgregarLog("Contrato encontrado: " + rs1.getString("contrato"));
                    System.out.println("Item Liquidacion: " + rs1.getString("itempago"));
                    Utilidades.AgregarLog("Item Liquidacion: " + rs1.getString("itempago"));
                    System.out.println("Precio Item: " + rs1.getString("precio"));
                    Utilidades.AgregarLog("Precio Item: " + rs1.getString("precio"));

                    if (rs.getString("anomalia").equals("")) {
                        System.out.println("Tipo de lectura OK");
                        Utilidades.AgregarLog("Tipo de lectura OK");
                        sql = "UPDATE lecturas SET contrato =?, item=?, precio=?, f_liquidacion=GETDATE(), usuario_liquidacion='robot', tipo_liquidacion=1 where id=?";
                        java.sql.PreparedStatement pst2 = conexion.getConnection().prepareStatement(sql);
                        pst2.setString(1, rs1.getString("contrato"));
                        pst2.setString(2, rs1.getString("itempago"));
                        pst2.setDouble(3, rs1.getDouble("precio"));
                        pst2.setLong(4, rs.getLong("id"));

                        if (conexion.Update(pst2) > 0) {
                            total1++;
                            conexion.Commit();
                        }
                    } else {
                        //Lectura con Anomalia
                        System.out.println("Tipo de lectura ANOMALA");
                        Utilidades.AgregarLog("Tipo de lectura ANOMALA");
                        sql = "SELECT anompago,anompopa, anomitem FROM anomalias WHERE anomcodi=? ";
                        java.sql.PreparedStatement pst2 = conexion.getConnection().prepareStatement(sql);
                        pst2.setString(1, rs.getString("anomalia"));
                        java.sql.ResultSet rs2 = conexion.Query(pst2);
                        if (rs2.next()) {  // Se encontro configuracion de anomalia
                            System.out.println("Codigo de Anomalia encontrada");
                            Utilidades.AgregarLog("Codigo de Anomalia encontrada");
                            if (rs2.getInt("anompago") == 0) {
                                //no se paga la anomalia
                                System.out.println("Lectura no se paga");
                                Utilidades.AgregarLog("Lectura no se paga");
                                sql = "UPDATE lecturas SET contrato =?, precio=0, f_liquidacion=GETDATE(), usuario_liquidacion='robot',tipo_liquidacion=2 where id=?";
                                java.sql.PreparedStatement pst3 = conexion.getConnection().prepareStatement(sql);
                                pst3.setString(1, rs1.getString("contrato"));
                                pst3.setLong(2, rs.getLong("id"));

                                if (conexion.Update(pst3) > 0) {
                                    total1++;
                                    conexion.Commit();
                                }

                            } else {
                                System.out.println("Lectura anomala se paga al " + rs2.getDouble("anompopa") + "%");
                                Utilidades.AgregarLog("Lectura anomala se paga al " + rs2.getDouble("anompopa") + "%");
                                sql = "UPDATE lecturas SET contrato =?, item=? , precio=?, f_liquidacion=GETDATE(), usuario_liquidacion='robot',tipo_liquidacion=? where id=?";
                                java.sql.PreparedStatement pst3 = conexion.getConnection().prepareStatement(sql);
                                pst3.setString(1, rs1.getString("contrato"));
                                pst3.setString(2, rs1.getString("itempago"));
                                pst3.setDouble(3, rs1.getDouble("precio") * (rs2.getDouble("anompopa") / 100));
                                if (rs2.getDouble("anompopa") != 100) {
                                    pst3.setInt(4, 2);  // Anomalia no se paga completa
                                } else {
                                    pst3.setInt(4, 1); // Anomalia se paga completa, como una LECTURA OK
                                }
                                pst3.setLong(5, rs.getLong("id"));

                                if (conexion.Update(pst3) > 0) {
                                    total1++;
                                    conexion.Commit();
                                }
                            }
                        } else {
                            System.out.println("Codigo de Anomalia NO encontrada");
                            Utilidades.AgregarLog("Codigo de Anomalia NO encontrada");
                        }
                    }
                }
            }
            
            AplicarPoliticaDirectos(periodo);
            fecha = new Date();
            System.out.println("Fin: " + fecha.toString());
            Utilidades.AgregarLog("Fin: " + fecha.toString());
            System.out.println("Total lecturas Liquidadas: " + total1);
            Utilidades.AgregarLog("Total lecturas Liquidadas: " + total1);

        } catch (SQLException e) {
            // TODO Auto-generated catch block
            System.out.println("Error de conexion con el servidor: " + e.getMessage());
            Utilidades.AgregarLog("Error: " + e.getMessage());
        } catch (Exception e) {
            // TODO Auto-generated catch block
            System.out.println("Error. " + e.getMessage());
            Utilidades.AgregarLog("Error: " + e.getMessage());
        } finally {
            if (conexion != null) {
                try {
                    // TODO Auto-generated catch block
                    conexion.Close();
                } catch (SQLException ex) {
                    Logger.getLogger(LiquidadorLecturas.class.getName()).log(Level.SEVERE, null, ex);
                    Utilidades.AgregarLog("Error: " + ex.getMessage());
                }
            }
        }
                
                

    }

    public static void Inicializar(String periodo) {
        System.out.println("Inicializando registros del periodo :" + periodo);
        Utilidades.AgregarLog("Inicializando registros del periodo :" + periodo);
        db conexion = null;
        try {
            conexion = new db();
            String sql = "UPDATE lecturas SET contrato = '', "
                    + "precio =0, "
                    + "excluido =0, "
                    + "duplicado = 0,"
                    + "item = '', "
                    + "f_liquidacion = null, "
                    + "usuario_liquidacion = '', "
                    + "tipo_liquidacion = 0,"
                    + "clasificacion = '',"
                    + "tipologia = '', "
                    + "liq_directos=0,"
                    + "mixto=0, "
                    + "fecha_clasificacion = null ,"
                    + "usuario_clasificacion = '' "
                    + " WHERE REPLACE(CONVERT(VARCHAR(7), f_lect, 120), '-', '') = ?";
            java.sql.PreparedStatement pst = conexion.getConnection().prepareStatement(sql);
            pst.setString(1, periodo);
            long total = conexion.Update(pst);
            if (total > 0) {
                conexion.Commit();
            }
            System.out.println("Total registros inicializados: " + total);
            Utilidades.AgregarLog("Total registros inicializados: " + total);

        } catch (SQLException e) {
            System.out.println("Error: " + e.getMessage());
            Utilidades.AgregarLog("Error: " + e.getMessage());
        } finally {
            if (conexion != null) {
                try {
                    conexion.Close();
                } catch (SQLException ex) {
                    Logger.getLogger(LiquidadorLecturas.class.getName()).log(Level.SEVERE, null, ex);
                    Utilidades.AgregarLog("Error: " + ex.getMessage());
                }
            }
        }
                    
                    

    }

    public static void Clasificar(String periodo) {
        System.out.println("Iniciando proceso de clasificacion de lecturas");
        Utilidades.AgregarLog("Iniciando proceso de clasificacion de lecturas");
        db conexion = null;
        long contador = 0;
        try {
            conexion = new db();
            String sql = "SELECT distinct unicom, num_itim,ruta FROM lecturas WHERE REPLACE(CONVERT(VARCHAR(7), f_lect, 120), '-', '') = ? ORDER BY unicom,ruta,num_itim";
            java.sql.PreparedStatement pst = conexion.getConnection().prepareStatement(sql);
            pst.setString(1, periodo);
            java.sql.ResultSet rs = conexion.Query(pst);
            while (rs.next()) {
                sql = "select clasificacion,tipologia,mixto from itinerario where unicom=? and ruta=? and itinerario=? and estado=1";
                java.sql.PreparedStatement pst2 = conexion.getConnection().prepareStatement(sql);
                pst2.setString(1, rs.getString("unicom"));
                pst2.setString(2, rs.getString("ruta"));
                pst2.setString(3, rs.getString("num_itim"));

                java.sql.ResultSet rs2 = conexion.Query(pst2);
                if (rs2.next()) {  // Se encontro el itinerario
                    System.out.println("Clasificando lecturas: Unicom: " + rs.getString("unicom") + " Ruta: " + rs.getString("ruta") + " Itinerario: " + rs.getString("num_itim"));
                    Utilidades.AgregarLog("Clasificando lecturas: Unicom: " + rs.getString("unicom") + " Ruta: " + rs.getString("ruta") + " Itinerario: " + rs.getString("num_itim"));
                    sql = "UPDATE lecturas SET clasificacion=?, tipologia=?, mixto=?, "
                            + "usuario_clasificacion='robot', fecha_clasificacion =SYSDATETIME() "
                            + " WHERE unicom=? AND ruta=? AND num_itim=? "
                            + " AND REPLACE(CONVERT(VARCHAR(7), f_lect, 120), '-', '') = ? ";

                    java.sql.PreparedStatement pst3 = conexion.getConnection().prepareStatement(sql);
                    pst3.setString(1, rs2.getString("clasificacion"));
                    pst3.setString(2, rs2.getString("tipologia"));
                    pst3.setString(3, rs2.getString("mixto"));
                    pst3.setString(4, rs.getString("unicom"));
                    pst3.setString(5, rs.getString("ruta"));
                    pst3.setString(6, rs.getString("num_itim"));
                    pst3.setString(7, periodo);
                    int filas = conexion.Update(pst3);
                    if (filas > 0) {
                        contador += filas;
                        System.out.println("Lecturas clasificadas: " + filas);
                        Utilidades.AgregarLog("Lecturas clasificadas: " + filas);
                        conexion.Commit();
                    }

                }

            }
        } catch (SQLException e) {
            System.out.println("Error: " + e.getMessage());
            Utilidades.AgregarLog("Error: " + e.getMessage());
        } finally {
            if (conexion != null) {
                try {
                    conexion.Close();
                } catch (SQLException e) {
                    // TODO Auto-generated catch block
                    System.out.println("Error: " + e.getMessage());
                }
            }
        }

        System.out.println("Proceso finalizado");
        Utilidades.AgregarLog("Proceso finalizado");
        System.out.println("Total lecturas clasificadas: " + contador);
        Utilidades.AgregarLog("Total lecturas clasificadas: " + contador);

    }

    public static void Excepciones(String periodo) {
        System.out.println("Iniciando proceso de exclusion de Excepciones");
        Utilidades.AgregarLog("Iniciando proceso de exclusion de Excepciones. Periodo " + periodo);
        db conexion = null;
        long total = 0;
        try {
            conexion = new db();

            String sql = "UPDATE lecturas SET lecturas.excluido = 1 "
                    + " WHERE lecturas.nis_rad IN (SELECT DISTINCT ex_suministros.suministro from ex_suministros WHERE tipo='L' and ex_suministros.periodo=? ) "
                    + " AND REPLACE(CONVERT(VARCHAR(7), f_lect, 120), '-', '') = ? ";

            java.sql.PreparedStatement pst = conexion.getConnection().prepareStatement(sql);
            pst.setString(1, periodo);
            pst.setString(2, periodo);
            total = conexion.Update(pst);
            if (total > 0) {
                conexion.Commit();

            }

            System.out.println("Total suministros Excentos: " + total);
            Utilidades.AgregarLog("Total suministros Excentos: " + total);
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            System.out.println("Error de conexion con el servidor: " + e.getMessage());
            Utilidades.AgregarLog("Error: " + e.getMessage());
        } catch (Exception e) {
            // TODO Auto-generated catch block
            System.out.println("Error. " + e.getMessage());
            Utilidades.AgregarLog("Error: " + e.getMessage());
        } finally {
            if (conexion != null) {
                try {
                    conexion.Close();
                } catch (SQLException e) {
                    // TODO Auto-generated catch block
                    System.out.println(e.getMessage());
                    Utilidades.AgregarLog("Error: " + e.getMessage());
                }
            }
        }

    }

    public static void Duplicados(String periodo) {
        System.out.println("Iniciando proceso de exclusion de duplicados");
        Utilidades.AgregarLog("Iniciando proceso de exclusion de duplicados. Periodo " + periodo);
        db conexion = null;
        long total = 0;
        try {
            conexion = new db();

            String sql = "select id,nis_rad "
                    + " FROM lecturas "
                    + " WHERE REPLACE(CONVERT(VARCHAR(7), f_lect, 120), '-', '') = ? "
                    + " ORDER BY nis_rad";

            java.sql.PreparedStatement pst = conexion.getConnection().prepareStatement(sql);
            pst.setString(1, periodo);

            java.sql.ResultSet rs = conexion.Query(pst);
            String nis_rad = "-1";
            while (rs.next()) {
                if (rs.getString("nis_rad").equals(nis_rad)) {
                    Utilidades.AgregarLog("Suministro duplicado " + nis_rad + " id " + rs.getLong("id"));
                    sql = "UPDATE lecturas SET duplicado=1 WHERE id=?";
                    java.sql.PreparedStatement pst1 = conexion.getConnection().prepareStatement(sql);
                    pst1.setLong(1, rs.getLong("id"));

                    if (conexion.Update(pst1) > 0) {
                        total++;
                        conexion.Commit();
                    }
                }

                nis_rad = rs.getString("nis_rad");
            }

            System.out.println("Total suministros Duplicados: " + total);
            Utilidades.AgregarLog("Total suministros Duplicados: " + total);
                    
                    

        } catch (SQLException e) {
            // TODO Auto-generated catch block
            System.out.println("Error de conexion con el servidor: " + e.getMessage());
        } catch (Exception e) {
            // TODO Auto-generated catch block
            System.out.println("Error. " + e.getMessage());
            Utilidades.AgregarLog("Error: " + e.getMessage());
        } finally {
            if (conexion != null) {
                try {
                    conexion.Close();
                } catch (SQLException e) {
                    // TODO Auto-generated catch block
                    System.out.println(e.getMessage());
                    Utilidades.AgregarLog("Error: " + e.getMessage());
                }
            }
        }

    }

    public static void AplicarPoliticaDirectos(String periodo) {
        db conexion = null;
        Utilidades.AgregarLog("Iniciando aplicación politica directos");
        System.out.println("Iniciando aplicación politica directos");
        

        try {
            conexion = new db();
            String sql = "SELECT unicom,ruta,num_itim, count(*) TOTAL,"
                    + " SUM(CASE WHEN num_apa='CONDIR' THEN 1 ELSE 0 END) DIRECTOS "
                + " FROM lecturas "
                + " WHERE REPLACE(CONVERT(VARCHAR(7), f_lect, 120), '-', '') = ? "
                + " GROUP BY unicom,ruta,num_itim "
                + " ORDER BY unicom,ruta,num_itim ";
            java.sql.PreparedStatement pst = conexion.getConnection().prepareStatement(sql);
            pst.setString(1, periodo);
            java.sql.ResultSet rs = conexion.Query(pst);
            while (rs.next()) {
                Utilidades.AgregarLog(String.format("Procesando Directos: unicom %s ruta %s itinerario %s",rs.getString("unicom"),rs.getString("ruta"),rs.getString("num_itim")));
                System.out.println(String.format("Procesando Directos: unicom %s ruta %s itinerario %s",rs.getString("unicom"),rs.getString("ruta"),rs.getString("num_itim")));
                Utilidades.AgregarLog(String.format("Total suministros: %s Total Directos: %s",rs.getLong("TOTAL"),rs.getLong("DIRECTOS") ));
                System.out.println(String.format("Total suministros: %s Total Directos: %s",rs.getLong("TOTAL"),rs.getLong("DIRECTOS") ));
                Utilidades.AgregarLog("Consultando configuracion Itinerario");
                System.out.println("Consultando configuracion Itinerario");
                sql = "SELECT pdirectos,pitemdirectos FROM itinerario WHERE unicom=? and ruta=? and itinerario=?";
                java.sql.PreparedStatement pst2 = conexion.getConnection().prepareStatement(sql);
                pst2.setString(1, rs.getString("unicom"));
                pst2.setString(2, rs.getString("ruta"));
                pst2.setString(3, rs.getString("num_itim"));
                
                java.sql.ResultSet rs2 = conexion.Query(pst2);
                if (rs2.next()) {
                    
                    int pDirectos = rs2.getInt("pdirectos");
                    int pItemDirectos  = rs2.getInt("pitemdirectos");
                    Utilidades.AgregarLog(String.format("Porcentaje directos: %s, Porcentaje del precio liquidado: %s",pDirectos,pItemDirectos));
                    System.out.println(String.format("Porcentaje directos: %s, Porcentaje del precio liquidado: %s",pDirectos,pItemDirectos));
                    if (pDirectos > 0) {
                        int totalItinerario = rs.getInt("TOTAL");
                        int totalDirectos = rs.getInt("DIRECTOS");
                        double porcentaje = Math.round(((double)totalDirectos/totalItinerario)*100);
                        Utilidades.AgregarLog(String.format("Porcentaje de directos directos: %s",porcentaje ));
                        System.out.println(String.format("Porcentaje de directos directos: %s",porcentaje ));
                        if ( porcentaje >= pDirectos) {  // Aplicar politica
                            Utilidades.AgregarLog(String.format("Itinerario Aplica porcentaje de directos directos: %s",porcentaje ));
                            System.out.println(String.format("Itinerario Aplica porcentaje de directos directos: %s",porcentaje ));
                            sql = "UPDATE lecturas SET precio = precio*(?/100), liq_directos=1 "
                                    + " WHERE unicom=? and ruta=? and num_itim=? "
                                    + " AND REPLACE(CONVERT(VARCHAR(7), f_lect, 120), '-', '') = ? "
                                    + " AND num_apa='CONDIR' and precio > 0";
                            java.sql.PreparedStatement pst3 = conexion.getConnection().prepareStatement(sql);
                            pst3.setInt(1, pItemDirectos);
                            pst3.setString(2, rs.getString("unicom"));
                            pst3.setString(3, rs.getString("ruta"));
                            pst3.setString(4, rs.getString("num_itim"));
                            pst3.setString(5, periodo);
                            long cnt = conexion.Update(pst3);
                            if ( cnt >= 0) {
                                conexion.Commit();
                                Utilidades.AgregarLog(String.format("Lecturas actualizadas con precio del %s : %s",pItemDirectos,cnt ));
                                System.out.println(String.format("Lecturas actualizadas con precio del %s : %s",pItemDirectos,cnt ));
                                
                            }
                            
                        }else {
                            Utilidades.AgregarLog(String.format("Itinerario NO APLICA porcentaje de directos directos: %s",porcentaje ));
                            System.out.println(String.format("Itinerario NO APLICA porcentaje de directos directos: %s",porcentaje ));
                            sql = "UPDATE lecturas SET precio = 0, liq_directos=2 "
                                    + " WHERE unicom=? and ruta=? and num_itim=? "
                                    + " AND REPLACE(CONVERT(VARCHAR(7), f_lect, 120), '-', '') = ? "
                                    + " AND num_apa='CONDIR' and precio > 0 ";
                            java.sql.PreparedStatement pst3 = conexion.getConnection().prepareStatement(sql);
                            pst3.setString(1, rs.getString("unicom"));
                            pst3.setString(2, rs.getString("ruta"));
                            pst3.setString(3, rs.getString("num_itim"));
                            pst3.setString(4, periodo);
                            
                            long cnt = conexion.Update(pst3);
                            if ( cnt >= 0) {
                                conexion.Commit();
                                Utilidades.AgregarLog(String.format("Lecturas actualizadas con precio cero(0): %s",cnt ));
                                System.out.println(String.format("Lecturas actualizadas con precio cero(0): %s",cnt  ));
                            }
                        }
                    
                    }
                }
                

            }

        } catch (SQLException e) {
            // TODO Auto-generated catch block
            System.out.println("Error de conexion con el servidor: " + e.getMessage());
        } catch (Exception e) {
            // TODO Auto-generated catch block
            System.out.println("Error. " + e.getMessage());
            Utilidades.AgregarLog("Error: " + e.getMessage());
        } finally {
            if (conexion != null) {
                try {
                    conexion.Close();
                } catch (SQLException e) {
                    // TODO Auto-generated catch block
                    System.out.println(e.getMessage());
                    Utilidades.AgregarLog("Error: " + e.getMessage());
                }
            }
        }
    }

}
