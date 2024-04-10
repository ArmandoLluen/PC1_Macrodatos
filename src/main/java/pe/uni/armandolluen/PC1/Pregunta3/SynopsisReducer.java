package pe.uni.armandolluen.PC1.Pregunta3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SynopsisReducer extends Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // Iteramos sobre los valores recibidos
        for (Text value : values) {
            // Emitimos el nombre como clave y la sinopsis como valor
            context.write(key, value);
        }
    }
}