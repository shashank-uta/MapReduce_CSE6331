import java.io.*;
import java.util.Hashtable;
import java.util.Scanner;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;



public class Twitter {

    public static class MyMapper1 extends Mapper<Object,Text,IntWritable,IntWritable>{
        @Override
        public void map( Object key, Text value, Context context) throws IOException, InterruptedException {
            Scanner scanner = new Scanner(value.toString()).useDelimiter(",");

            int value1 = scanner.nextInt();
            int key1 = scanner.nextInt();

            context.write(new IntWritable(key1), new IntWritable(value1));
            scanner.close();
        }
    }

    public static class Combiner1 extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable>{

        protected void minireduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            for(IntWritable value : values){
                count = count + 1;
            }

            IntWritable size = new IntWritable(count);

            context.write(key, size);
        }
    }

    public static class MyReducer1 extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {

        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException,InterruptedException {
            int count = 0;

            for(IntWritable value : values){
                count = count + 1;
            }

            IntWritable size = new IntWritable(count);

            context.write(key, size);
        }

    }

    public static class MyMapper2 extends Mapper<Object,Text,IntWritable,IntWritable> {
        static Hashtable<Integer, Integer> H = new Hashtable<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            H = new Hashtable<Integer, Integer>();
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {

            H.forEach((k, v) -> {
                try {
                    context.write(new IntWritable(k),new IntWritable(v));
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            });
        }

        public void map(Object key,Text value,Context context) throws IOException, InterruptedException{
            Scanner scanner = new Scanner(value.toString()).useDelimiter("\t");
            int v1 = scanner.nextInt();
            int size = scanner.nextInt();

            if(H.get(size) == null){
                H.put(size, 1);
            }else{
                H.put(size, H.get(size) + 1);
            }
           // context.write(new IntWritable(size), new IntWritable(1));
            scanner.close();
        }

    }

    public static class MyReducer2 extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {

        public void reduce(IntWritable key,Iterable<IntWritable> values, Context context) throws IOException,InterruptedException {

            int sum = 0;

            for(IntWritable value : values) {
                sum = sum + value.get();
            }

            context.write(key, new IntWritable(sum));
        }
    }



    public static void main ( String[] args ) throws Exception {



        Job job1 = Job.getInstance();
        job1.setJobName("MyJob1");
        job1.setJarByClass(Twitter.class);
        job1.setOutputKeyClass(IntWritable.class);
        job1.setOutputValueClass(IntWritable.class);
        job1.setMapOutputKeyClass(IntWritable.class);
        job1.setMapOutputValueClass(IntWritable.class);
        job1.setMapperClass(MyMapper1.class);
        job1.setCombinerClass(Combiner1.class);
        job1.setReducerClass(MyReducer1.class);
        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job1,new Path(args[0]));
        FileOutputFormat.setOutputPath(job1,new Path(args[1]));
        job1.waitForCompletion(true);

        Job job2 = Job.getInstance();
        job2.setJobName("Myjob2");
        job2.setJarByClass(Twitter.class);
        job2.setOutputKeyClass(IntWritable.class);
        job2.setOutputValueClass(IntWritable.class);
        job2.setMapOutputKeyClass(IntWritable.class);
        job2.setMapOutputValueClass(IntWritable.class);
        job2.setMapperClass(MyMapper2.class);
        job2.setReducerClass(MyReducer2.class);
        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job2,new Path(args[1]));
        FileOutputFormat.setOutputPath(job2,new Path(args[2]));
        job2.waitForCompletion(true);


    }
}