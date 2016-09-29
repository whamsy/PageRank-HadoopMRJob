import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PageRank {
        public static void main(String[] args) throws Exception {
                int i = 0;
                String opfile = "/part-r-00000";
                Path opath = new Path(opfile);
                Path inputPath = new Path(args[0]);
                Path outputPath = new Path(args[1] );
                while (i < 3){

                        String suffix = ("/"+String.valueOf(i));
                        Path spath = new Path(suffix);
                        Job job = getNewJob(i);
                        i++;
                        job.setJarByClass(PageRank.class);
                        FileInputFormat.addInputPath(job, inputPath);
                        FileOutputFormat.setOutputPath(job, outputPath);
                        job.setMapperClass(PageRankMapper.class);
                        job.setReducerClass(PageRankReducer.class);
                        job.setOutputKeyClass(Text.class);
                        job.setOutputValueClass(Text.class);
                        inputPath = Path.mergePaths(outputPath,opath);
                        outputPath = Path.mergePaths(outputPath,spath);
                        job.waitForCompletion(true);
                }

        }
        private static Job getNewJob(int i) throws IOException {
                Job job = new Job();
                job.setJobName("Page Rank "+ String.valueOf(i));
                return job;
        }
}