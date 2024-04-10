package pe.uni.armandolluen.PC1.Pregunta6;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;

public class KMeansReducer1 extends Reducer<IntWritable, Text, IntWritable, Text> {

    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // Calcular el nuevo centroide para el cluster actual
        double[] newCentroid = calculateNewCentroid(values);

        // Emitir el cluster y el nuevo centroide
        context.write(key, new Text(arrayToString(newCentroid)));
    }

    // Método para calcular el nuevo centroide para un cluster
    private double[] calculateNewCentroid(Iterable<Text> values) {
        // Inicializar la suma de las características
        double[] sum = new double[3];
        int count = 0;

        // Sumar las características de todos los puntos en el cluster
        for (Text value : values) {
            String[] tokens = value.toString().split(",");
            sum[0] += Double.parseDouble(tokens[8]);
            sum[1] += Double.parseDouble(tokens[9]);
            sum[2] += Double.parseDouble(tokens[10]);
            count++;
        }

        // Calcular el promedio de las características para obtener el nuevo centroide
        double[] newCentroid = new double[3];
        if (count != 0) {
            newCentroid[0] = sum[0] / count;
            newCentroid[1] = sum[1] / count;
            newCentroid[2] = sum[2] / count;
        }

        return newCentroid;
    }

    // Método para convertir un array a una cadena de texto
    private String arrayToString(double[] array) {
        StringBuilder sb = new StringBuilder();
        for (double value : array) {
            sb.append(value).append(",");
        }
        return sb.toString();
    }
}