package greensas.dao;

import greensas.utilidades.ConexionBD;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;



public class BodegaDao
{
    private final ConexionBD conexionBD;


    public BodegaDao(ConexionBD conexionBD)
    {
        this.conexionBD = conexionBD;
    }


    /**
     * Método que permite obtener todos las bodegas del SIIGO
     *
     * @param unidadNegocio
     * @param bodegas
     * @return Lista con todas las bodegas
     * @throws Exception
     */
    public List<String> getAllBodegas(String unidadNegocio, String bodegas) throws Exception
    {
        List<String> lstBodegas = new ArrayList<String>();

        try
        {
            String consulta = "select b.NroBodegaBod, b.NombreBod from TABLA_DESCRIPCION_BODEGAS b where b.NroBodegaBod in(" + bodegas + ")";

            ResultSet resultSet = conexionBD.ejecutarConsulta(consulta);

            while (resultSet.next())
            {
                try
                {   
                    lstBodegas.add(resultSet.getString(1).trim().concat("|").concat(resultSet.getString(2).trim()).concat("|").concat(unidadNegocio));
                }
                catch (SQLException e)
                {
                    Logger.getLogger(BodegaDao.class.getName()).log(Level.SEVERE, "Error: No es posible agregar la bodega a la colección", e);
                }
            }
        }
        catch (Exception e)
        {
            Logger.getLogger(BodegaDao.class.getName()).log(Level.SEVERE, "Error: No fue posible ejecutar la consulta de bodegas", e);
            throw (e);
        }
        finally
        {
            conexionBD.cerrarConsulta();
        }

        return lstBodegas;
    }
}
