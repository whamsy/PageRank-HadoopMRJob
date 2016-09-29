import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PageRankMapper
        extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{

                String line = value.toString();
                line = line.replaceAll("\\s+",";");
                StringTokenizer token = new StringTokenizer(line, ";");
                ArrayList<String> list = new ArrayList<String>();
                while (token.hasMoreTokens()) {
                        list.add(token.nextToken());
                }
                int size = list.size();
                double ipr = Double.parseDouble(list.get(size-1)); //initial page rank
                String pageid = list.get(0); // pageid is always first element
                int numlinks = (size-2); // number of output links = size  - (first and last)
                double opr = ipr/(double)numlinks;  //output page rank = total/number of links
                String oprtext = (pageid + " " +String.valueOf(opr));
                int loop = 1;

                String outputlinks = "";

                while(loop <= numlinks) {

                       outputlinks += (list.get(loop)+" ");

                       context.write(new Text(list.get(loop)), new Text(oprtext));
                       loop++;
                }

                context.write(new Text(pageid), new Text(outputlinks));
        }
}
