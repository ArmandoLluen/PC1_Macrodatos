package pe.uni.armandolluen.PC1.Pregunta4.a;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import pe.uni.armandolluen.PC1.Pregunta4.AnimeData;

import java.io.IOException;

public class RegressionMapper extends Mapper<LongWritable, Text, Text, AnimeData> {

    private Text outputKey = new Text();
    private AnimeData outputValue = new AnimeData();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split(",", -1); // Split CSV by comma and keep empty values

        // Check if all required fields are present
        if (fields.length >= 12) {
            try {
                double score = Double.parseDouble(fields[8]);
                double popularity = Double.parseDouble(fields[11]);

                // Set values to AnimeData object
                outputValue.setScore(score);
                outputValue.setPopularity(popularity);

                // Emit key-value pair
                context.write(outputKey, outputValue);
            } catch (NumberFormatException e) {
                // Log or handle the exception accordingly
                // In this case, we'll just skip this record
                System.err.println("Failed to parse score or popularity: " + e.getMessage());
            }
        }
    }
}