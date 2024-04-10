package pe.uni.armandolluen.PC1.Pregunta6;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;

public class KMeansMapper2 extends Mapper<LongWritable, Text, IntWritable, Text> {

    private double[][] centroids;

    @Override
    protected void setup(Context context) {
        // Cargar los centroides desde el contexto
        centroids = loadCentroids(context.getConfiguration());
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Parsear la línea CSV
        String[] tokens = value.toString().split(",");

        try {
            // Obtener las características relevantes
            double[] features = {Double.parseDouble(tokens[8]), Double.parseDouble(tokens[9]), Double.parseDouble(tokens[10])};

            // Inicializar la mínima distancia y el índice del cluster asignado
            double minDistance = Double.MAX_VALUE;
            int assignedCluster = -1;

            // Calcular la distancia a cada centroide y asignar al cluster más cercano
            for (int i = 0; i < centroids.length; i++) {
                double distance = calculateDistance(features, centroids[i]);
                if (distance < minDistance) {
                    minDistance = distance;
                    assignedCluster = i;
                }
            }

            // Emitir la asignación de cluster
            context.write(new IntWritable(assignedCluster), value);
        }catch (NumberFormatException ignored){

        }
    }

    // Cargar los centroides desde la configuración
    private double[][] loadCentroids(Configuration conf) {
        return new double[][]{
                {7.2, 1050, 480},
                {7.8, 1520, 320},
                {5.8, 820, 710},
                {9.1, 1980, 120},
                {6.4, 1190, 420}
        };
    }

    // Calcular la distancia euclidiana entre dos puntos
    private double calculateDistance(double[] point1, double[] point2) {
        double sum = 0;
        for (int i = 0; i < point1.length; i++) {
            sum += Math.pow((point1[i] - point2[i]), 2);
        }
        return Math.sqrt(sum);
    }
}