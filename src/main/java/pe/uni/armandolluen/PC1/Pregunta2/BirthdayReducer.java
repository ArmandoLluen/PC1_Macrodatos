package pe.uni.armandolluen.PC1.Pregunta2;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class BirthdayReducer extends Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        // Emitir todos los registros que cumplen con el filtro
        for (Text value : values) {
            context.write(key, value);
        }
    }
}