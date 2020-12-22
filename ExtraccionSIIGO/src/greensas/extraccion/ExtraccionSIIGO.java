package greensas.extraccion;

import greensas.dao.BodegaDao;
import greensas.dao.CiudadDao;
import greensas.dao.ClienteDao;
import greensas.dao.VendedorDao;
import greensas.dao.VentasDao;
import greensas.utilidades.ConexionBD;
import greensas.utilidades.Constantes;
import greensas.utilidades.Tools;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.HashMap;
import java.util.Map;




public class ExtraccionSIIGO
{

    private static ConexionBD conexionBD;

    private static int año;
    private static String unidadNegocio;
    private static final String bodegas = Constantes.BODEGAS_CALI;
    private static Map<String, String> hsmProductosHomologados;

    //Si necesitamos hacer cargas incrementales utilizariamos la linea de abajo
    //private static String path = "src/" + Integer.toString(fecha.get(Calendar.YEAR)) + "/" + Integer.toString(fecha.get(Calendar.DAY_OF_MONTH)) + "." + Integer.toString(fecha.get(Calendar.MONTH)+1) + "." + Integer.toString(fecha.get(Calendar.YEAR));    
    private static final String PATH = "D:\\BIQlik\\greensas-integracion\\ExtraccionSIIGO\\src\\Data\\";


    public static void main(String[] args)
    {
        /**
         * Recorriendo unidades de negocio dependiendo la ciudad. Pasto maneja
         * dos unidades de negocio por separado EDS y Restaurantes Cali maneja
         * una unidad de negocio EDS
         */
        for (String unidadNegocioTemp : getAllUnidadesNegocio(Constantes.CALI))
        {
            boolean isGenerar = true;

            //Obteniendo productos que van hacer homologados
            hsmProductosHomologados = Tools.leerArchivoMap(PATH + "ProductosHomologados.txt");

            año = Calendar.getInstance().get(Calendar.YEAR);
            unidadNegocio = unidadNegocioTemp;

            //Recorriendo año actual y año anterior
            for (int j = 0; j < 2; j++)
            {
                try
                {
                    conexionBD = new ConexionBD(unidadNegocio + "_" + año);

                    /**
                     * Se valida que se genere una sola ves esta información, ya
                     * que es la misma para cada año
                     */
                    if (isGenerar)
                    {
                        generarCiudades();
                        generarVendedores();
                        generarBodegas();
                        generarProductos();
                        generarClientes();

                        isGenerar = false;
                    }

                    System.out.println("Generando ventas del año [" + año + "]");
                    generarVentas();
                    
                    //Crea archivo de ventas totales para efecto de pruebas
//                    generarVentas2();
                    System.out.println("Finalizo ventas del año [" + año + "]");

                    conexionBD.cerrarConexion();
                    waitCerrarConexion();
                    año -= 1;
                }
                catch (Exception ex)
                {
                    año -= 1;
                    Logger.getLogger(ExtraccionSIIGO.class.getName()).log(Level.SEVERE, "Error: No es posible generar conexión a la BD", ex);
                }
            }
        }
    }


    /**
     * Método que permite generar el archivo de ciudades del SIIGO
     */
    private static void generarCiudades()
    {
        try
        {
            CiudadDao ciudadDao = new CiudadDao(conexionBD);
            List<String> lstCiudades = ciudadDao.getAllCiudadades(unidadNegocio);

            if (!lstCiudades.isEmpty())
            {
//                lstCiudades.add(0, "NroCiudadCiu|CodDaneCiu|NombreCiu|UnidadNegocio");
                lstCiudades.add(0, "ciudades|ciudad|unidad_negocio"); //se renombrean las cabeceras para efectos en Qlik
                Tools.crearArchivo(lstCiudades, PATH + "Ciudades" + unidadNegocio + ".txt");
            }
        }
        catch (Exception ex)
        {
            Logger.getLogger(ExtraccionSIIGO.class.getName()).log(Level.SEVERE, "Error: No es posible generar el archivo de ciudades", ex);
        }
    }


    /**
     * Método que permite generar el archivo de vendedores del SIIGO
     */
    private static void generarVendedores()
    {
        try
        {
            VendedorDao vendedorDao = new VendedorDao(conexionBD);
            List<String> lstVendedores = vendedorDao.getAllVendedores(unidadNegocio);

            if (!lstVendedores.isEmpty())
            {
//                lstVendedores.add(0, "VenVen|Coordinador|NombreVen|UnidadNegocio");
                lstVendedores.add(0, "vendedores|vendedor|unidad_negocio"); //se renombrean las cabeceras para efectos en Qlik
                Tools.crearArchivo(lstVendedores, PATH + "Vendedores" + unidadNegocio + ".txt");
            }
        }
        catch (Exception ex)
        {
            Logger.getLogger(ExtraccionSIIGO.class.getName()).log(Level.SEVERE, "Error: No es posible generar el archivo de vendedores", ex);
        }
    }


    /**
     * Método que permite generar el archivo de bodegas del SIIGO
     */
    private static void generarBodegas()
    {
        try
        {
            BodegaDao bodegaDao = new BodegaDao(conexionBD);
            List<String> lstBodegas = bodegaDao.getAllBodegas(unidadNegocio, bodegas);

            if (!lstBodegas.isEmpty())
            {
//                lstBodegas.add(0, "NroBodegaBod|NombreBod|UnidadNegocio");
                lstBodegas.add(0, "tiendas|tienda|unidad_negocio"); //se renombrean las cabeceras para efectos en Qlik
                Tools.crearArchivo(lstBodegas, PATH + "Bodegas" + unidadNegocio + ".txt");
            }
        }
        catch (Exception ex)
        {
            Logger.getLogger(ExtraccionSIIGO.class.getName()).log(Level.SEVERE, "Error: No es posible generar el archivo de bodegas", ex);
        }
    }


    /**
     * Método que permite generar el archivo de productos del SIIGO
     */
    private static void generarProductos()
    {
        try
        {
//            ProductoDao productoDao = new ProductoDao(conexionBD);
//            List<String> lst = productoDao.getAllProductos();

            Map<String, String> hsmProductos = new HashMap<String, String>();
            List<String> lstProductos = new ArrayList<String>();

            List<String> lstProductosTemp = Tools.leerArchivo(PATH + "Productos" + unidadNegocio + "_Siigo.txt");

            for (int i = 0; i < lstProductosTemp.size(); i++)
            {
                String campos[] = lstProductosTemp.get(i).split("\\|");

                String idProducto = campos[0]; //5700016000102

                /**
                 * Si el producto siigo esta en el listado de productos
                 * homologados, se debe cambiar el idproducto viejo por el nuevo
                 */
                if (hsmProductosHomologados.containsKey(idProducto))
                {
                    idProducto = hsmProductosHomologados.get(idProducto);
                }

                String descripcion = campos[1].replace(" 16%", "").replace(" 19%", "");
                String linea = campos[2].replace(" 16%", "");
                String grupo = campos[3].replace(" 19%", "").replace(" 16%", "").replace(" 5%", "");
                String idLinea = idProducto.substring(0, 3); //570
                String idGrupo = idProducto.substring(3, 7); //0016
                String tipo = campos[8];
                String proveedor = campos[4].replace(",", ".");
                String precio1 = campos[5].replace(".", ",");
                String precio3 = campos[6].replace(".", ",");
                String costo = campos[7].replace(".", ",");
                String descodificar = "N";
                Double unidadConversion = 1.0;

                if ("0016".equals(idGrupo) || "0019".equals(idGrupo))
                {
                    idProducto = idLinea.concat(idProducto.substring(7)); //570000102
                }

                if (descripcion.toUpperCase().contains("NO USAR"))
                {
                    descodificar = "S";
                    descripcion = descripcion.replace("NO USAR ", "");
                }
                else
                {
                    if (hsmProductos.containsKey(idProducto))
                    {
                        descodificar = "N";
                    }
                }

                try
                {
                    //Para las EDS, el campos proveedor es utilizado como comodin para colocar el factor de conversion
                    unidadConversion = Double.valueOf(proveedor);
                }
                catch (NumberFormatException e)
                {
                    /**
                     * Error controlado en caso de que en el campo proveedor, no venga un dato numérico, entonces se asume como 1 la unidad de conversion la cual en Qlik sera multiplicada por la cantidad...
                     */
                }

                String registro = idProducto.concat("|").concat(tipo).concat("|").concat(linea).concat("|").concat(idGrupo).concat("|").concat(grupo).concat("|").concat(descripcion).concat("|").concat(proveedor).concat("|").concat(precio1).concat("|").concat(precio3).concat("|").concat(descodificar).concat("|").concat(costo).concat("|").concat(unidadNegocio).concat("|").concat(unidadConversion.toString().replace(".", ","));

                hsmProductos.put(idProducto, registro);
            }

            for (String registro : hsmProductos.values())
            {
                lstProductos.add(registro);
            }

//            lstProductos.add(0, "ID_PRODUCTO|TIPO|ID_LINEA|LINEA|ID_GRUPO|GRUPO|PRODUCTO|PROVEEDOR|PRECIO1|PRECIO3|DESCODIFICADO|COSTO|UNIDADNEGOCIO|UNIDADCONVERSION");
            lstProductos.add(0, "productos|tipo|linea|id_grupo|grupo|producto|proveedor|precio1|precio3|descodificado|costo|unidad_negocio|unidad_conversion"); //se renombrean las cabeceras para efectos en Qlik
            Tools.crearArchivo(lstProductos, PATH + "Productos" + unidadNegocio + ".txt");
        }
        catch (Exception ex)
        {
            Logger.getLogger(ExtraccionSIIGO.class.getName()).log(Level.SEVERE, "Error: No es posible leer el archivo de productos.", ex);
        }
    }


    private static void generarClientes()
    {
        try
        {
            ClienteDao clienteDao = new ClienteDao(conexionBD);
            
            List<String> lstClientes = new ArrayList<String>();
            List<String> lstClientesBD = clienteDao.getAllClientes(unidadNegocio);

            if (!lstClientesBD.isEmpty())
            {
                for (String cliente : lstClientesBD)
                {
                    String tipoCliente = "CONTADO";
                    
                    cliente = cliente.replace("\"", "");
                    String datosCliente[] = cliente.split("\\|");
                    
                    String nit = datosCliente[0];
                    String nombreCliente = datosCliente[1];
                    String tipoClienteBD = datosCliente[2].trim().toUpperCase();
                    
                    if(tipoClienteBD.contains("CREDITO"))
                    {
                        tipoCliente = "CREDITO";
                    }
                    else
                    {
                        if(tipoClienteBD.contains("PREPAGO"))
                        {
                            tipoCliente = "PREPAGO";
                        }
                    }
                    
                    lstClientes.add(nit.concat("|").concat(nombreCliente).concat("|").concat(tipoCliente).concat("|").concat(unidadNegocio));
                }

//                lstClientes.add(0, "Nit|NombreCliente|TipoCliente|UnidadNegocio");
                lstClientes.add(0, "clientes|cliente|tipo_cliente|unidad_negocio"); //se renombrean las cabeceras para efectos en Qlik
                Tools.crearArchivo(lstClientes, PATH + "Clientes" + unidadNegocio + ".txt");
            }
        }
        catch (Exception ex)
        {
            Logger.getLogger(ExtraccionSIIGO.class.getName()).log(Level.SEVERE, "Error: No es posible generar el archivo de clientes", ex);
        }
    }


    private static void generarVentas2()
    {
        try
        {
            System.out.println("Generando " + PATH + año + "_" + unidadNegocio + "_DataVentasTotal.txt");
            VentasDao ventasDao = new VentasDao(conexionBD);
            List<String> lstVentas = ventasDao.getAllVentas(bodegas);
            lstVentas.add(0, "idProducto|idBodega|cantidad|valor|idVendedor|fechaVenta|tipo|ano|mes|dia|ordenTipo|DcMov|cuenta|nit");
            Tools.crearArchivo(lstVentas, PATH + año + "_" + unidadNegocio + "_DataVentasTotal.txt");
        }
        catch (Exception ex)
        {
            Logger.getLogger(ExtraccionSIIGO.class.getName()).log(Level.SEVERE, null, ex);
        }
    }


    /**
     * Método que permite generar el archivo de ventas del SIIGO
     */
    private static void generarVentas()
    {
        try
        {
            VentasDao ventasDao = new VentasDao(conexionBD);

            List<String> lstVentasSaldos = new ArrayList<String>();
            List<String> lstDevoluciones = new ArrayList<String>();

            List<String> lstVentas = ventasDao.getAllVentas(bodegas);
//            List<String> lstVentas = Tools.leerArchivo(PATH + "2019_EDS_DataVentasTotal.txt");

            double saldo = 0;
            int ultimoInventario = 1;

            for (String venta : lstVentas)
            {
                String campos[] = venta.split("\\|");

                try
                {
                    String tipoMovimiento = campos[6];
                    String valor = campos[3];

                    if (!(("F".equals(tipoMovimiento) || "J".equals(tipoMovimiento)) && "0".equals(valor)))
                    {
                        String idProducto = campos[0];

                        /**
                         * Si el producto de la venta esta en el listado de
                         * productos homologados se debe cambiar el idproducto
                         * viejo por el nuevo
                         */
                        if (hsmProductosHomologados.containsKey(idProducto))
                        {
                            idProducto = hsmProductosHomologados.get(idProducto);
                        }

                        String idLinea = idProducto.substring(0, 3); //570
                        String idGrupo = idProducto.substring(3, 7); //0016
                        String idBodega = campos[1];
                        double cantidad = Constantes.CANTIDAD_ZERO;
                        String idVendedor = campos[4];
                        String fechaVenta = campos[5];
                        String dcMov = campos[11];
                        String nit = campos[13];
                        double cantidadComprada = Constantes.CANTIDAD_ZERO;
                        String registro = "";

                        /**
                         * Cuando el producto pertenece al grupo del 16% y 19%
                         * debemos recortar dicho grupo del codigo para efectos
                         * de unificar el producto, ya que es el mismo producto,
                         * no son dos diferentes
                         */
                        if ("0016".equals(idGrupo) || "0019".equals(idGrupo))
                        {
                            idProducto = idLinea.concat(idProducto.substring(7));
                        }

                        try
                        {
                            cantidad = Double.parseDouble(campos[2]);
                        }
                        catch (NumberFormatException nfe)
                        {
                            //Excepción controlada en caso de que la cantidad no sea numérica
                            cantidad = Constantes.CANTIDAD_ZERO;
                        }

                        switch (tipoMovimiento)
                        {
                            case "SA":
                                saldo = cantidad;
                                break;

                            default:
                                if ("C".equals(dcMov))
                                {
                                    saldo -= cantidad;
                                }
                                else
                                {
                                    if ("D".equals(dcMov))
                                    {
                                        saldo += cantidad;
                                    }
                                }

                                if ("F".equals(tipoMovimiento))
                                {
//                                    System.out.println("idProducto: " + idProducto + " Fecha: " + fechaVenta + " TipoMovimiento: " + tipoMovimiento + " Cantidad: " + cantidad + " dcMov: " + dcMov);
                                    registro = idProducto.concat("|").concat(idBodega).concat("|").concat(String.valueOf(cantidad).replace(".", ",")).concat("|").concat(valor.replace(".", ",")).concat("|").concat(idVendedor).concat("|").concat(fechaVenta).concat("|").concat(String.valueOf(saldo).replace(".", ",")).concat("|").concat("" + ultimoInventario).concat("|").concat(String.valueOf(cantidadComprada).replace(".", ",")).concat("|").concat(unidadNegocio).concat("|").concat(nit);
                                    lstVentasSaldos.add(registro);

                                    ultimoInventario++;
                                }
                                else
                                {
                                    if ("J".equals(tipoMovimiento) || "E".equals(tipoMovimiento) || "O".equals(tipoMovimiento) || "T".equals(tipoMovimiento))
                                    {
                                        String camposVenta[] = lstVentasSaldos.get(lstVentasSaldos.size() - 1).split("\\|");

                                        //Se valida que sea el mismo producto para aplicar el nuevo saldo al registro anterior
                                        if (idProducto.equals(camposVenta[0]))
                                        {
                                            cantidadComprada = Constantes.CANTIDAD_ZERO;

                                            /**
                                             * Se validad que el tipo de
                                             * movimiento sea E, que significa
                                             * que es una Entrada o Compra de un
                                             * prodcuto, de lo contrario la
                                             * cantidad comprada deberá ser cero
                                             * para cualquier tipo de movimiento
                                             */
                                            if ("E".equals(tipoMovimiento))
                                            {
                                                cantidadComprada = cantidad;
                                            }

                                            String registroVenta = camposVenta[0].concat("|").concat(camposVenta[1]).concat("|").concat(camposVenta[2]).concat("|").concat(camposVenta[3]).concat("|").concat(camposVenta[4]).concat("|").concat(camposVenta[5]).concat("|").concat(String.valueOf(saldo).replace(".", ",")).concat("|").concat(camposVenta[7]).concat("|").concat(String.valueOf(cantidadComprada).replace(".", ",")).concat("|").concat(unidadNegocio).concat("|").concat(camposVenta[10]);

                                            lstVentasSaldos.set((lstVentasSaldos.size() - 1), registroVenta);
                                        }

                                        if ("D".equals(dcMov) && "J".equals(tipoMovimiento))
                                        {
//                                            if ("70".equals(idBodega))
//                                            {
//                                                System.out.println("idProducto: " + idProducto + "Fecha: " + fechaVenta + " TipoMovimiento: " + tipoMovimiento + " Cantidad: " + cantidad + " dcMov: " + dcMov);
//                                            }
                                            lstDevoluciones.add(idProducto.concat(idBodega).concat(String.valueOf(cantidad).replace(".", ",")).concat(fechaVenta.substring(0, 6)));
                                        }
                                    }
                                }
                                break;
                        }
                    }
                }
                catch (StringIndexOutOfBoundsException | ArrayIndexOutOfBoundsException e)
                {
                    /**
                     * Error controlado...
                     */
                }
            }

            for (String devolucion : lstDevoluciones)
            {
                for (int i = 0; i < lstVentasSaldos.size(); i++)
                {
                    String campos[] = lstVentasSaldos.get(i).split("\\|");
                    String venta = campos[0].concat(campos[1]).concat(campos[2]).concat(campos[5].substring(0, 6));

                    if (venta.contains(devolucion))
                    {
                        lstVentasSaldos.remove(i);
                        break;
                    }
                }
            }

            if (!lstVentasSaldos.isEmpty())
            {
//                lstVentasSaldos.add(0, "ProductoMov|NroBodegaBod|CantidadMov|ValorMov|VenVen|FechaDctoMov|Saldo|Rank|CantidadComprada|UnidadNegocio|Nit");
                lstVentasSaldos.add(0, "productos|tiendas|cantidad|valor|vendedores|id_tiempo|saldo|rank|cantidad_comprada|unidad_megocio|clientes"); //se renombrean las cabeceras para efectos en Qlik
                Tools.crearArchivo(lstVentasSaldos, PATH + año + "_" + unidadNegocio + "_Ventas.txt");
            }
        }
        catch (Exception ex)
        {
            Logger.getLogger(ExtraccionSIIGO.class.getName()).log(Level.SEVERE, "Error: No es posible generar el archivo de ventas", ex);
        }
    }


    /**
     * Método que permite obtener el numero de unidades de negocio de la
     * compañia, según la ciudad
     *
     * @param ciudad Ciudad donde tiene cede
     * @return Lista con el prefijo de la unidad de negocio, este prefijo es
     * utilizado para armar el nombre del ODBC
     */
    private static List<String> getAllUnidadesNegocio(String ciudad)
    {
        List<String> lstUnidadesNegocio = new ArrayList<>();
        lstUnidadesNegocio.add(Constantes.UNIDAD_NEGOCIO_EDS);

        switch (ciudad)
        {
            case Constantes.PASTO:
                lstUnidadesNegocio.add(Constantes.UNIDAD_NEGOCIO_RES);
                break;

            default:
                break;
        }

        return lstUnidadesNegocio;
    }


    /**
     * Método que permite dar una espera para poder cerrar la conexión de SIIGO
     * ODBC
     */
    private static void waitCerrarConexion()
    {
        try
        {
            Thread.sleep((long) 30000);
        }
        catch (InterruptedException ex)
        {
            Logger.getLogger(ExtraccionSIIGO.class.getName()).log(Level.SEVERE, "Error: en el Wait...", ex);
        }
    }
}
