package pe.uni.armandolluen.PC1.Pregunta1;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ScoreMapper extends Mapper<Object, Text, Text, DoubleWritable> {

    private Text word = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(","); // Suponiendo que el separador es una coma
        try {
            double score = Double.parseDouble(fields[1]); // Suponiendo que 'score' está en el quinto campo
            word.set("score");
            context.write(word, new DoubleWritable(score));
        } catch (NumberFormatException e) {
            // Manejar el caso en el que el texto no se pueda convertir a un número
            // Ignoramos la línea en este caso
        }
    }
}
