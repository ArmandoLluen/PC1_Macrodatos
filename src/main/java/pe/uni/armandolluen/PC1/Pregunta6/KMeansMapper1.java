package pe.uni.armandolluen.PC1.Pregunta6;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;

public class KMeansMapper1 extends Mapper<LongWritable, Text, IntWritable, Text> {

    private static final int NUM_CLUSTERS = 5; // Número de clusters
    private static final double MAX_VALUE = Double.MAX_VALUE;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Parsear la línea CSV
        String[] tokens = value.toString().split(",");

        // Obtener las características relevantes (por ejemplo, score, scored_by, rank)
        try {
            double[] features = {Double.parseDouble(tokens[8]), Double.parseDouble(tokens[9]), Double.parseDouble(tokens[10])};

            // Inicializar las distancias mínimas y el índice del cluster asignado
            double minDistance = MAX_VALUE;
            int assignedCluster = -1;

            // Calcular la distancia a cada centroide y asignar al cluster más cercano
            for (int i = 0; i < NUM_CLUSTERS; i++) {
                // Calcular la distancia euclidiana
                double distance = calculateDistance(features, centroids[i]);

                // Actualizar si la distancia es menor
                if (distance < minDistance) {
                    minDistance = distance;
                    assignedCluster = i;
                }
            }

            // Emitir la asignación de cluster para el siguiente paso de K-means
            context.write(new IntWritable(assignedCluster), value);
        }catch (NumberFormatException ignored){

        }
    }

    // Método para calcular la distancia euclidiana entre dos puntos en un espacio n-dimensional
    private double calculateDistance(double[] point1, double[] point2) {
        double sum = 0;
        for (int i = 0; i < point1.length; i++) {
            sum += Math.pow((point1[i] - point2[i]), 2);
        }
        return Math.sqrt(sum);
    }

    // Centroides iniciales (pueden ser generados aleatoriamente o inicializados de otro modo)
    private final double[][] centroids = {
            {7.5, 1000, 500},
            {8.0, 1500, 300},
            {6.0, 800, 700},
            {9.0, 2000, 100},
            {6.5, 1200, 400}
    };
}
