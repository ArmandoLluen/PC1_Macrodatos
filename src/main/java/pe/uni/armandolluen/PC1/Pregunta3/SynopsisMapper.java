package pe.uni.armandolluen.PC1.Pregunta3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class SynopsisMapper extends Mapper<Object, Text, Text, Text> {

    private Text outputKey = new Text();
    private Text outputValue = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // Utilizar una expresión regular mejorada para dividir la línea en campos de CSV
        String[] fields = value.toString().split(",(?=(?:[^'\"]*(['\"])[^'\"]*\1)*[^'\"]*$)", -1);

        // Asegurarse de que haya suficientes campos y que la sinopsis contenga la palabra "criminal"
        if (fields.length >= 6 && fields[5].toLowerCase().contains("criminal")) {
            // Eliminar las comillas de los campos si es necesario
            String name = removeQuotes(fields[1]);
            String synopsis = removeQuotes(fields[5]);

            // Emitir el nombre como clave y la sinopsis como valor
            outputKey.set(name);
            outputValue.set(synopsis);
            context.write(outputKey, outputValue);
        }
    }

    // Método para eliminar las comillas de un campo si están presentes
    private String removeQuotes(String field) {
        if (field.startsWith("\"") && field.endsWith("\"")) {
            return field.substring(1, field.length() - 1);
        }
        return field;
    }
}