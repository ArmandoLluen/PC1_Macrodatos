package pe.uni.armandolluen.PC1.Pregunta1;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ScoreReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
            throws IOException, InterruptedException {

        double sum = 0;
        double sumOfSquares = 0;
        double min = Double.MAX_VALUE;
        double max = Double.MIN_VALUE;
        int count = 0;

        // Iterating through all values for the key
        for (DoubleWritable val : values) {
            double score = val.get();
            sum += score;
            sumOfSquares += score * score;
            if (score < min) {
                min = score;
            }
            if (score > max) {
                max = score;
            }
            count++;
        }

        // Calculate mean
        double mean = sum / count;

        // Calculate standard deviation
        double variance = (sumOfSquares / count) - (mean * mean);
        double stdDev = Math.sqrt(variance);

        // Emitimos los resultados
        context.write(new Text("Numero de Registros"), new DoubleWritable(count));
        context.write(new Text("Promedio"), new DoubleWritable(mean));
        context.write(new Text("Desviacion Estandar"), new DoubleWritable(stdDev));
        context.write(new Text("Max"), new DoubleWritable(max));
        context.write(new Text("Min"), new DoubleWritable(min));
    }
}
