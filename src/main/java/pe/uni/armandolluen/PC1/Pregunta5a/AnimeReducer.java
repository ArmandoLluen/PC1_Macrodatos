package pe.uni.armandolluen.PC1.Pregunta5a;

import java.io.IOException;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AnimeReducer extends Reducer<Text, FloatWritable, Text, Text> {

    private final Text outputKey = new Text();
    private final Text outputValue = new Text();

    public void reduce(Text key, Iterable<FloatWritable> values, Context context)
            throws IOException, InterruptedException {
        float sumScore = 0;
        int countScore = 0;
        float sumPopularity = 0;
        int countPopularity = 0;

        String[] keyParts = key.toString().split("\\|");
        String animeId = keyParts[0];
        String title = keyParts[1];
        String gender = keyParts[2];

        for (FloatWritable value : values) {
            if (key.toString().endsWith("score")) {
                sumScore += value.get();
                countScore++;
            } else {
                sumPopularity += value.get();
                countPopularity++;
            }
        }

        float avgScore = sumScore / countScore;
        float avgPopularity = sumPopularity / countPopularity;

        outputKey.set("anime_id: " + animeId + ", title: " + title);
        outputValue.set("Gender: " + gender + ", Score average: " + avgScore + " | Popularity average: " + avgPopularity);
        context.write(outputKey, outputValue);
    }
}