package greensas.utilidades;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;



public class Tools
{

    /**
     * Método que permite crear el archivo plano
     *
     * @param lstData Lista de campos a imprimir en el archivo
     * @param nombreArchivo Archivo a crear
     * @throws java.lang.Exception
     */
    public static void crearArchivo(List<String> lstData, String nombreArchivo) throws Exception
    {
        BufferedWriter out = null;

        try
        {
            eliminarArchivo(nombreArchivo);
            out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(nombreArchivo), "iso-8859-1"));

            for (String linea : lstData)
            {
                out.write(linea);
                out.newLine();
            }
        }
        catch (Exception e)
        {
            throw (e);
        }
        finally
        {
            if (null != out)
            {
                out.flush();
                out.close();
            }
        }
    }


    /**
     * Método que permite eliminar el archivo plano
     *
     * @param nombreArchivo Archivo a eliminar
     */
    public static void eliminarArchivo(String nombreArchivo)
    {
        (new File(nombreArchivo)).delete();
    }


    /**
     * Método que permite leer un archivo plano omitiendo la cabecera como
     * primera linea
     *
     * @param nombreArchivo Path y nombre del archivo
     * @return Una lista con cada linea
     * @throws Exception
     */
    public static List<String> leerArchivo(String nombreArchivo)
    {
        BufferedReader br = null;
        List<String> lstData = new ArrayList<String>();

        try
        {
            br = new BufferedReader(new InputStreamReader(new FileInputStream(nombreArchivo), "iso-8859-1"));

            //Se lee la primer linea que representa la cabecera
            String linea = br.readLine();

            // Lectura del fichero
            while ((linea = br.readLine()) != null)
            {
                lstData.add(linea);
            }
        }
        catch (Exception ex)
        {
            return lstData;
        }
        finally
        {
            // En el finally cerramos el fichero, para asegurarnos
            // que se cierra tanto si todo va bien como si salta 
            // una excepcion.
            try
            {
                if (null != br)
                {
                    br.close();
                }
            }
            catch (IOException ex)
            {
                return lstData;
            }
        }

        return lstData;
    }


    public static Map<String, String> leerArchivoMap(String nombreArchivo) 
    {
        BufferedReader br = null;
        Map<String, String> lstData = new HashMap<String, String>();

        try
        {
            br = new BufferedReader(new InputStreamReader(new FileInputStream(nombreArchivo), "iso-8859-1"));

            //Se lee la primer linea que representa la cabecera
            String linea = br.readLine();

            // Lectura del fichero
            while ((linea = br.readLine()) != null)
            {
                String campos[] = linea.split("\\|");

                //campos[0] = key
                //campos[1] = value
                lstData.put(campos[0], campos[1]);
            }
        }
        catch (Exception ex)
        {
            return lstData;
        }
        finally
        {
            // En el finally cerramos el fichero, para asegurarnos
            // que se cierra tanto si todo va bien como si salta 
            // una excepcion.
            try
            {
                if (null != br)
                {
                    br.close();
                }
            }
            catch (IOException ex)
            {
                return lstData;
            }
        }

        return lstData;
    }
}
