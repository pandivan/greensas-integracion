package greensas.dao;

import greensas.utilidades.ConexionBD;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;



public class VendedorDao
{
    private final ConexionBD conexionBD;


    public VendedorDao(ConexionBD conexionBD)
    {
        this.conexionBD = conexionBD;
    }


    /**
     * Método que permite obtener todos los vendedores del SIIGO
     *
     * @param unidadNegocio
     * @return Lista con todos los vendedores
     * @throws Exception
     */
    public List<String> getAllVendedores(String unidadNegocio) throws Exception
    {
        List<String> lstVendedores = new ArrayList<String>();

        try
        {
            String consulta = "SELECT ven.VenVen, 'Magda Rosero' Coordinador, ven.NombreVen "
                            + "FROM TABLA_DESCRIPCION_VENDEDORES ven";

            ResultSet resultSet = conexionBD.ejecutarConsulta(consulta);

            while (resultSet.next())
            {
                try
                {   
                    lstVendedores.add(resultSet.getString(1).trim().concat("|").concat(resultSet.getString(2).trim()).concat("|").concat(resultSet.getString(3).trim()).concat("|").concat(unidadNegocio));
                }
                catch (SQLException e)
                {
                    Logger.getLogger(VendedorDao.class.getName()).log(Level.SEVERE, "Error: No es posible agregar el vendedor a la colección", e);
                }
            }
        }
        catch (Exception e)
        {
            Logger.getLogger(VendedorDao.class.getName()).log(Level.SEVERE, "Error: No fue posible ejecutar la consulta de vendedores", e);
            throw (e);
        }
        finally
        {
            conexionBD.cerrarConsulta();
        }

        return lstVendedores;
    }
}
