import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PageRankReducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

                float fpr = 0.0f;
                String outlinks ="";

                for (Text value : values){

                        String line = value.toString();

                        if (line.matches(".*\\d.*")){

                                String[] token = line.split(" ");
                                fpr += Float.parseFloat(token[1]);
                        } else {

                              outlinks += line;
                        }
                }

                String output = (outlinks +String.valueOf(fpr));
                context.write(key, new Text(output));

        }
}