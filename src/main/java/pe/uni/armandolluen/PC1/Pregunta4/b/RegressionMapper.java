package pe.uni.armandolluen.PC1.Pregunta4.b;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import pe.uni.armandolluen.PC1.Pregunta4.AnimeData;

public class RegressionMapper extends Mapper<LongWritable, Text, Text, AnimeData> {

    private Text outputKey = new Text();
    private AnimeData outputValue = new AnimeData();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split(",");

        // Check if all required fields are present
        if (fields.length >= 12) {
            try {
                // Extracting relevant fields
                double popularity = Double.parseDouble(fields[11]);
                double rank = Double.parseDouble(fields[10]);

                // Set values to output value
                outputKey.set("Regression");
                outputValue.setPopularity(popularity);
                outputValue.setRank(rank);

                // Emitting key-value pair
                context.write(outputKey, outputValue);
            } catch (NumberFormatException e) {
                // Skip this record if parsing fails
                // You can log or handle the error according to your requirement
            }
        }
    }
}
