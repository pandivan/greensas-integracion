package greensas.utilidades;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ConexionBD 
{
    private Connection conexion = null;
    private Statement statement = null;
    private ResultSet resultSet = null;
    
    
    public ConexionBD(String año) throws Exception 
    {
        if (null == conexion) 
        {
            try 
            {
                Class.forName("sun.jdbc.odbc.JdbcOdbcDriver").newInstance();
                conexion = DriverManager.getConnection("jdbc:odbc:SIIGO_" + año);
            } 
            catch (ClassNotFoundException | InstantiationException | IllegalAccessException | SQLException e) 
            {
                /**
                 * TODO: Este ODBC solo funciona con JAVA 7, favor instalarlo y
                 * funcionará automáticamente
                 */
                Logger.getLogger(ConexionBD.class.getName()).log(Level.SEVERE, "Error: No es posible abrir la conexión.", e);
                throw (e);
            } 
        }
    }
    
    
    
    /**
     * Método que permite ejecutar una consulta en BD
     * @param consulta a ejecutar
     * @return El resultado de la consulta sobre la BD
     * @throws java.lang.Exception
     */
    public ResultSet ejecutarConsulta(String consulta) throws Exception
    {
        resultSet = null;
        try 
        {
            statement = conexion.createStatement();
            resultSet = statement.executeQuery(consulta);
        } 
        catch (Exception e) 
        {
            Logger.getLogger(ConexionBD.class.getName()).log(Level.SEVERE, "Error: No es posible ejecutar la consulta.", e);
            throw (e);
        }
        
        return resultSet;
    }
    
    
    
    /**
     * Método que permite desconectar la conexión a la BD 
     */
    public void desconectar()
    {
        cerrarConsulta();
        cerrarConexion();
    }
    
    
    
    /**
     * Método que permite cerrar la consulta.
     */
    public void cerrarConsulta()
    {
        if (null != resultSet) 
        {
            try 
            {
                resultSet.close();
            } 
            catch (Exception e) 
            {
                Logger.getLogger(ConexionBD.class.getName()).log(Level.SEVERE, "Error: No es posible cerrar la consulta.", e);
            }
        }
    }
 
    
    /**
     * Método que permite cerrar la conexión.
     */
    public void cerrarConexion()
    {
        if (null != statement)
        {
            try 
            {
                statement.close();
                conexion = null;
            }
            catch (Exception e) 
            {
                Logger.getLogger(ConexionBD.class.getName()).log(Level.SEVERE, "Error: No es posible cerrar la conexión.", e);
            }
        }
    }
}
