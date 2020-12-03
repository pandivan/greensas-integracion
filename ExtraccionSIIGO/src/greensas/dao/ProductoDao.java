package greensas.dao;

import greensas.utilidades.ConexionBD;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;



public class ProductoDao
{

    private final ConexionBD conexionBD;


    public ProductoDao(ConexionBD conexionBD)
    {
        this.conexionBD = conexionBD;
    }


    /**
     * Método que permite obtener todos los productos del SIIGO
     *
     * @return Lista con todos los productos
     * @throws Exception
     */
    public List<String> getAllProductos() throws Exception
    {
        List<String> lstProductos = new ArrayList<String>();

        try
        {
//            String consulta = "select p.CodTinv, p.LinTinv, p.NomLinTinv, p.GruTinv, p.NomGruTinv, p.NombreRegTbl "
//                    + "from TABLA_DESCRIPCION_INVENTARIOS p ";
//
//            String consulta = "select p.NomLinTinv "
//                    + "from TABLA_DESCRIPCION_INVENTARIOS p ";

            String consulta = "select DISTINCT p.ProductoMov, p.LinMov, p.GrpMov, p.ProMov, p.DescrMov "
                    + "from TABLA_MOV_INVEN_POR_COMPROBANTE p "
                    + "where p.TipMov = 'F' and p.CuentasMov = '4135201005'";

            ResultSet resultSet = conexionBD.ejecutarConsulta(consulta);

            while (resultSet.next())
            {
                try
                {
                    lstProductos.add(resultSet.getString(1).concat("|").concat(resultSet.getString(2).trim()).concat("|").concat(resultSet.getString(3).trim()).concat("|").concat(resultSet.getString(4).trim()).concat("|").concat(resultSet.getString(5).trim()));
                }
                catch (SQLException e)
                {
                    Logger.getLogger(ProductoDao.class.getName()).log(Level.SEVERE, "Error: No es posible agregar el producto a la colección", e);
                }
            }
        }
        catch (Exception e)
        {
            Logger.getLogger(ProductoDao.class.getName()).log(Level.SEVERE, "Error: No fue posible ejecutar la consulta de productos", e);
            throw (e);
        }
        finally
        {
            conexionBD.cerrarConsulta();
        }

        return lstProductos;
    }
}
