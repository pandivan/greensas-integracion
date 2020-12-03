package greensas.dao;

import greensas.utilidades.ConexionBD;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;




public class ClienteDao
{
    private final ConexionBD conexionBD;


    public ClienteDao(ConexionBD conexionBD)
    {
        this.conexionBD = conexionBD;
    }

    /**
     * Método que permite obtener todos los clientes del SIIGO
     *
     * @param unidadNegocio
     * @return Lista con todos los ciudadades
     * @throws Exception
     */
    public List<String> getAllClientes(String unidadNegocio) throws Exception
    {
        List<String> lstClientes = new ArrayList<String>();

        try
        {
            //String consulta = "SELECT n.NitNit, n.SucNit, n.NombreNit as NombreCliente, p.NombrePag TipoCliente FROM TABLA_NITS_VARIOS n, TABLA_DESCRIPCION_FORMA_PAGO p where n.FormaPagoNit = p.TipoPag";
            String consulta = "SELECT n.NitNit, n.SucNit, n.NombreNit as NombreCliente, p.NombrePag TipoCliente FROM TABLA_NITS_VARIOS n, TABLA_DESCRIPCION_FORMA_PAGO p where n.FormaPagoNit = p.TipoPag";
            ResultSet resultSet = conexionBD.ejecutarConsulta(consulta);

            while (resultSet.next())
            {
                try
                {
                    lstClientes.add(resultSet.getString(1).trim().concat(resultSet.getString(2).trim()).concat("|").concat(resultSet.getString(3).trim()).concat("|").concat(resultSet.getString(4).trim()));
                }
                catch (SQLException e)
                {
                    Logger.getLogger(ClienteDao.class.getName()).log(Level.SEVERE, "Error: No es posible agregar el cliente a la colección", e);
                }
            }
        }
        catch (Exception e)
        {
            Logger.getLogger(ClienteDao.class.getName()).log(Level.SEVERE, "Error: No fue posible ejecutar la consulta de clientes", e);
            throw (e);
        }
        finally
        {
            conexionBD.cerrarConsulta();
        }

        return lstClientes;
    }
}
