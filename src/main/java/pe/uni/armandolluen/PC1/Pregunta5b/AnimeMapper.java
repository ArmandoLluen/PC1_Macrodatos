package pe.uni.armandolluen.PC1.Pregunta5b;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AnimeMapper extends Mapper<Object, Text, Text, FloatWritable> {

    private final Text outputKey = new Text();
    private final FloatWritable scorePopularityValue = new FloatWritable();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split(",");
        if (tokens.length >= 13) {
            String scoreStr = tokens[8].trim();
            String popularityStr = tokens[11].trim();
            for (int i = 13; i < tokens.length; i++) {
                String gender = tokens[i].trim();
                if (!gender.isEmpty() && !scoreStr.isEmpty() && !popularityStr.isEmpty()) {
                    try {
                        float score = Float.parseFloat(scoreStr);
                        float popularity = Float.parseFloat(popularityStr);
                        float scorePopularity = score * 1000 + popularity; // Para mantener ambos valores en un solo FloatWritable
                        outputKey.set("gender|" + gender);
                        scorePopularityValue.set(scorePopularity);
                        context.write(outputKey, scorePopularityValue);
                    }catch (NumberFormatException ignored){

                    }
                }
            }
        }
    }
}
