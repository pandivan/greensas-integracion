package greensas.dao;

import greensas.utilidades.ConexionBD;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;



public class CiudadDao
{
    private final ConexionBD conexionBD;


    public CiudadDao(ConexionBD conexionBD)
    {
        this.conexionBD = conexionBD;
    }

    /**
     * Método que permite obtener todos los ciudadades del SIIGO
     *
     * @param unidadNegocio
     * @return Lista con todos los ciudadades
     * @throws Exception
     */
    public List<String> getAllCiudadades(String unidadNegocio) throws Exception
    {
        List<String> lstCiudadades = new ArrayList<String>();

        try
        {
            String consulta = "SELECT c.NroCiudadCiu, c.CodDaneCiu, c.NombreCiu FROM TABLA_DESCRIPCION_CIUDAD c";

            ResultSet resultSet = conexionBD.ejecutarConsulta(consulta);

            while (resultSet.next())
            {
                try
                {   
                    lstCiudadades.add(resultSet.getString(1).trim().concat("|").concat(resultSet.getString(2).trim()).concat("|").concat(resultSet.getString(3).trim()).concat("|").concat(unidadNegocio));
                }
                catch (SQLException e)
                {
                    Logger.getLogger(CiudadDao.class.getName()).log(Level.SEVERE, "Error: No es posible agregar la ciudad a la colección", e);
                }
            }
        }
        catch (Exception e)
        {
            Logger.getLogger(CiudadDao.class.getName()).log(Level.SEVERE, "Error: No fue posible ejecutar la consulta de ciudades", e);
            throw (e);
        }
        finally
        {
            conexionBD.cerrarConsulta();
        }

        return lstCiudadades;
    }
}
