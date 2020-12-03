package greensas.dao;

import greensas.utilidades.ConexionBD;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;



public class VentasDao
{

    private final ConexionBD conexionBD;


    public VentasDao(ConexionBD conexionBD)
    {
        this.conexionBD = conexionBD;
    }


    /**
     * Método que permite obtener todas las ventas del SIIGO
     *
     * @param bodegas
     * @return Lista con todas las ventas
     * @throws Exception
     */
    public List<String> getAllVentas(String bodegas) throws Exception
    {
        List<String> lstVentas = new ArrayList<String>();

        try
        {
            String consulta = "SELECT mb.ProductoBod AS idProducto, mb.BodegaBod AS idBodega, mb.CantAntBod AS cantidad, mb.ValAntBod AS valor, -1 AS idVendedor, 20170101 AS fechaVenta, 'SA' AS tipo, mb.AnoCompraBod AS ano, 1 AS mes, 1 AS dia, 1 AS ordenTipo, 'X' AS DcMov, 'XXXXXXXXXXXX' AS cuenta, mb.ProductoBod AS nit, mb.BodegaBod AS SucMov  FROM TABLA_MAESTRO_BODEGAS mb where mb.BodegaBod in (" + bodegas + ") union all "
                    + "SELECT v.ProductoMov AS idProducto, v.BodegaMov AS idBodega, v.CantidadMov AS cantidad, v.ValorMov AS valor, v.VendedorMov AS idVendedor, v.FechaDctoMov AS fechaVenta, v.TipMov AS tipo, v.AnoDctoMov AS ano, v.MesDctoMov AS mes, v.DiaDctoMov AS dia, 2 AS ordenTipo, v.DcMov, v.CuentasMov AS cuenta, v.NitMov AS nit, v.SucMov FROM TABLA_MOV_INVEN_POR_COMPROBANTE v WHERE v.BodegaMov in (" + bodegas + ") and v.CuentasMov not like '6135%' ORDER BY 2, 1, 8, 9, 10, 11";

            ResultSet resultSet = conexionBD.ejecutarConsulta(consulta);

            while (resultSet.next())
            {
                try
                {
                    lstVentas.add(resultSet.getString(1).trim().concat("|").concat(resultSet.getString(2).trim()).concat("|").concat(resultSet.getString(3).trim()).concat("|").concat(resultSet.getString(4).trim()).concat("|").concat(resultSet.getString(5).trim()).concat("|").concat(resultSet.getString(6).trim()).concat("|").concat(resultSet.getString(7).trim()).concat("|").concat(resultSet.getString(8).trim()).concat("|").concat(resultSet.getString(9).trim()).concat("|").concat(resultSet.getString(10).trim()).concat("|").concat(resultSet.getString(11).trim()).concat("|").concat(resultSet.getString(12).trim()).concat("|").concat(resultSet.getString(13).trim()).concat("|").concat(resultSet.getString(14).trim().concat(resultSet.getString(15).trim())));
                }
                catch (SQLException e)
                {
                    Logger.getLogger(VentasDao.class.getName()).log(Level.SEVERE, "Error: No es posible agregar la venta a la colección", e);
                }
            }
        }
        catch (Exception e)
        {
            Logger.getLogger(VentasDao.class.getName()).log(Level.SEVERE, "Error: No fue posible ejecutar la consulta de ventas", e);
            throw (e);
        }
        finally
        {
            conexionBD.cerrarConsulta();
        }

        return lstVentas;
    }
}
