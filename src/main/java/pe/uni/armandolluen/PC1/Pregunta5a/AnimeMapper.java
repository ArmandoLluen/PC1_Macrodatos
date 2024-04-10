package pe.uni.armandolluen.PC1.Pregunta5a;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AnimeMapper extends Mapper<Object, Text, Text, FloatWritable> {

    private final Text outputKey = new Text();
    private final FloatWritable outputValue = new FloatWritable();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split(",");
        if (tokens.length >= 13) {
            String animeId = tokens[1];
            String title = tokens[5];
            String gender = tokens[4];
            String scoreStr = tokens[8].trim();
            String popularityStr = tokens[11].trim();

            // Comprobamos si hay columnas con espacios en blanco
            if (!animeId.isEmpty() && !title.isEmpty() && !gender.isEmpty() && !scoreStr.isEmpty() && !popularityStr.isEmpty()) {
                try {
                    float score = Float.parseFloat(scoreStr);
                    float popularity = Float.parseFloat(popularityStr);

                    outputKey.set(animeId + "|" + title + "|" + gender);
                    outputValue.set(score);
                    context.write(outputKey, outputValue);

                    outputKey.set(animeId + "|" + title + "|" + gender);
                    outputValue.set(popularity);
                    context.write(outputKey, outputValue);
                }catch (NumberFormatException ignored){

                }
            }
        }
    }
}
