package pe.uni.armandolluen.PC1.Pregunta5b;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class AnimeReducer extends Reducer<Text, FloatWritable, Text, Text> {

    private final Text outputValue = new Text();

    public final void reduce(Text key, Iterable<FloatWritable> values, Context context)
            throws IOException, InterruptedException {
        int count = 0;
        float totalScore = 0;
        float totalPopularity = 0;
        for (FloatWritable value : values) {
            float scorePopularity = value.get();
            float score = scorePopularity / 1000;
            float popularity = scorePopularity % 1000;
            count++;
            totalScore += score;
            totalPopularity += popularity;
        }
        float avgScore = totalScore / count;
        float avgPopularity = totalPopularity / count;
        outputValue.set("Gender: " + key.toString().split("\\|")[1] + ", Count: " + count + ", Score Average: " + avgScore + ", Popularity Average: " + avgPopularity);
        context.write(key, outputValue);
    }
}

