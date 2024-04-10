package pe.uni.armandolluen.PC1.Pregunta4.b;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import pe.uni.armandolluen.PC1.Pregunta4.AnimeData;

public class RegressionReducer extends Reducer<Text, AnimeData, Text, Double> {

    @Override
    protected void reduce(Text key, Iterable<AnimeData> values, Context context)
            throws IOException, InterruptedException {
        // Variables for summing up values
        double sumX = 0.0;
        double sumY = 0.0;
        double sumXY = 0.0;
        double sumXX = 0.0;
        int count = 0;

        // Calculating sums
        for (AnimeData data : values) {
            double popularity = data.getPopularity();
            double rank = data.getRank();

            sumX += rank;
            sumY += popularity;
            sumXY += rank * popularity;
            sumXX += rank * rank;
            count++;
        }

        // Calculating regression coefficients
        double b1 = (count * sumXY - sumX * sumY) / (count * sumXX - sumX * sumX);
        double b0 = (sumY - b1 * sumX) / count;

        // Calculating correlation coefficient
        double r = (count * sumXY - sumX * sumY) / Math.sqrt((count * sumXX - sumX * sumX) * (count * sumY - sumY * sumY));

        // Calculating coefficient of determination (R^2)
        double rSquare = r * r;

        // Emitting the coefficients
        context.write(new Text("Regression Coefficient (b1)"), b1);
        context.write(new Text("Intercept (b0)"), b0);
        context.write(new Text("Correlation Coefficient (r)"), r);
        context.write(new Text("Coefficient of Determination (R^2)"), rSquare);
    }
}
