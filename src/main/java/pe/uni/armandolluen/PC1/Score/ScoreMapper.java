package pe.uni.armandolluen.PC1.Score;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

// Clase Mapper
public class ScoreMapper extends Mapper<LongWritable, Text, Text, DoubleWritable>{

    private final static DoubleWritable score = new DoubleWritable();
    private Text title = new Text();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Dividir la línea en tokens usando coma como separador
        String[] tokens = value.toString().split(",");

        // Asegurarse de que la línea tenga todos los campos necesarios
        if (tokens.length == 14) {
            // Ignorar la primera línea que contiene los encabezados
            if (!tokens[0].equals("username")) {
                // Obtener el título del anime
                title.set(tokens[6]);

                // Convertir el puntaje a un número flotante si es un valor numérico
                try {
                    double parsedScore = Double.parseDouble(tokens[8]);
                    score.set(parsedScore);
                    context.write(title, score);
                } catch (NumberFormatException e) {
                    // Manejar valores no numéricos, por ejemplo, imprimir un mensaje de error
                    System.err.println("Error al convertir el puntaje a número flotante: " + tokens[8]);
                    // Continuar con el siguiente registro
                    return;
                }
            }
        }
    }
}

