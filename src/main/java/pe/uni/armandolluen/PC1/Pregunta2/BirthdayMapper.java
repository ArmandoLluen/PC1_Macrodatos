package pe.uni.armandolluen.PC1.Pregunta2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class BirthdayMapper extends Mapper<LongWritable, Text, Text, Text> {

    private final Text outputKey = new Text();
    private final Text outputValue = new Text();

    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        String[] parts = line.split(",");

        // Manejar errores si no hay suficientes columnas en la línea o si alguna columna está en blanco
        if (parts.length < 15 || containsBlankColumn(parts)) {
            System.err.println("Formato incorrecto para la línea o columna en blanco: " + line);
            return;
        }

        // Obtener la columna de cumpleaños
        String birthday = parts[3];

        // Extraer el año del cumpleaños y manejar errores de conversión de año
        String[] dateParts = birthday.split("-");
        if (dateParts.length < 1) {
            System.err.println("Formato incorrecto de fecha de cumpleaños en la línea: " + line);
            return;
        }
        String yearString = dateParts[0];
        int year;
        try {
            year = Integer.parseInt(yearString);
        } catch (NumberFormatException e) {
            System.err.println("Error al convertir el año en la línea: " + line);
            return;
        }

        // Filtrar los registros por año de cumpleaños (2004 y 2005)
        if (year >= 2004 && year <= 2005) {
            // Obtener las columnas requeridas: Mal ID, Username, Gender, Location
            String malId = parts[0];
            String username = parts[1];
            String gender = parts[2];
            String location = parts[4];

            // Emite las columnas requeridas como un cuadro
            StringBuilder box = new StringBuilder();
            box.append("+-------------+----------------------+-------+-------------------+\n");
            box.append(String.format("| %-11s| %-20s| %-5s | %-17s|\n", "Mal ID", "Username", "Gender", "Location"));
            box.append("+-------------+----------------------+-------+-------------------+\n");
            box.append(String.format("| %-11s| %-20s| %-5s | %-17s|\n", malId, username, gender, location));
            box.append("+-------------+----------------------+-------+-------------------+\n");

            outputKey.set("");
            outputValue.set(box.toString());
            context.write(outputKey, outputValue);
        }
    }

    // Método para verificar si alguna columna está en blanco
    private boolean containsBlankColumn(String[] parts) {
        for (String part : parts) {
            if (part.trim().isEmpty()) {
                return true;
            }
        }
        return false;
    }
}
