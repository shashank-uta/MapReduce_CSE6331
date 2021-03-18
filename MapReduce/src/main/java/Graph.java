import java.io.*;
import java.util.Scanner;
import java.util.Vector;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


class Tagged implements Writable {
    public boolean tag;                // true for a graph vertex, false for distance
    public int distance;               // the distance from the starting vertex
    public Vector<Integer> following;  // the vertex neighbors

    Tagged () {
        tag = false;
    }
    Tagged ( int d ) {
        tag = false; distance = d;
    }
    Tagged ( int d, Vector<Integer> f ) {
        tag = true; distance = d; following = f;
    }

    public void write ( DataOutput out ) throws IOException {
        out.writeBoolean(tag);
        out.writeInt(distance);
        if (tag) {
            out.writeInt(following.size());
            for ( int i = 0; i < following.size(); i++ )
                out.writeInt(following.get(i));
        }
    }

    public void readFields ( DataInput in ) throws IOException {
        tag = in.readBoolean();
        distance = in.readInt();
        if (tag) {
            int n = in.readInt();
            following = new Vector<Integer>(n);
            for ( int i = 0; i < n; i++ )
                following.add(in.readInt());
        }
    }
}

public class Graph {
    static int start_id = 14701391;
    static int max_int = Integer.MAX_VALUE;

    public static class MyMapper1 extends Mapper<Object,Text,IntWritable,IntWritable>{
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Scanner scanner = new Scanner(value.toString()).useDelimiter(",");

            int id = scanner.nextInt(); // it gives us who is being followed, this is value for our MyReducer1
            int followerId = scanner.nextInt(); // it gives us who is following other ids,
            // this is key for our MyReducer1

            context.write(new IntWritable(followerId), new IntWritable(id));
            scanner.close();
        }
    }

    public static class MyReducer1 extends Reducer<IntWritable,IntWritable,IntWritable,Tagged> {
        @Override
        protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            Vector<Integer> following = new Vector<>();

            for(IntWritable value : values) {
                following.add(value.get());
            }

            if(key.get() == start_id || key.get() == 1) {
                context.write(key, new Tagged(0,following));
                System.out.println(key + "->" + following.toString());
            } else {
                context.write(key, new Tagged(max_int, following));
                System.out.println(key + "->" + following.toString());
            }
        }
    }

    public static class MyMapper2 extends Mapper<IntWritable,Tagged,IntWritable,Tagged> {
        @Override
        protected void map(IntWritable key, Tagged value, Context context) throws IOException, InterruptedException {
            context.write(key, value);

            if(value.distance < max_int) {
                for(Integer values : value.following) {
                    context.write(new IntWritable(values), new Tagged(value.distance + 1));
                }
            }
        }
    }


    public static class MyReducer2 extends Reducer<IntWritable,Tagged,IntWritable,Tagged> {

        @Override
        protected void reduce(IntWritable key, Iterable<Tagged> values, Context context) throws IOException, InterruptedException {
            Integer m = max_int;
            Vector<Integer> following = new Vector<>();

            for(Tagged value : values) {
                if(value.distance < m) {
                    m = value.distance;
                }
                if(value.tag) {
                    following = value.following;
                }
            }
            context.write(key, new Tagged(m,following));
        }
    }

    public static class MyMapper3 extends Mapper<IntWritable,Tagged,IntWritable,IntWritable> {

        @Override
        protected void map(IntWritable key, Tagged value, Context context) throws IOException, InterruptedException {
            if(value.distance < max_int) {
                context.write(key, new IntWritable(value.distance));
            }
        }
    }

    /* ... */

    public static void main ( String[] args ) throws Exception {
        int iterations = 5;
        Job job1 = Job.getInstance();
        job1.setJobName("MyJob1");
        job1.setJarByClass(Graph.class);
        job1.setOutputKeyClass(IntWritable.class);
        job1.setOutputValueClass(Tagged.class);
        job1.setMapOutputKeyClass(IntWritable.class);
        job1.setMapOutputValueClass(IntWritable.class);
        job1.setMapperClass(MyMapper1.class);
        job1.setReducerClass(MyReducer1.class);
        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.setInputPaths(job1,new Path(args[0]));
        FileOutputFormat.setOutputPath(job1,new Path(args[1]+"0"));
        job1.waitForCompletion(true);


        for ( short i = 0; i < iterations; i++ ) {
            Job job2 = Job.getInstance();
            job2.setJobName("MyJob2");
            job2.setJarByClass(Graph.class);
            job2.setOutputKeyClass(IntWritable.class);
            job2.setOutputValueClass(Tagged.class);
            job2.setMapOutputKeyClass(IntWritable.class);
            job2.setMapOutputValueClass(Tagged.class);
            job2.setMapperClass(MyMapper2.class);
            job2.setReducerClass(MyReducer2.class);
            job2.setInputFormatClass(SequenceFileInputFormat.class);
            job2.setOutputFormatClass(SequenceFileOutputFormat.class);
            FileInputFormat.setInputPaths(job2,new Path(args[1]+i));
            FileOutputFormat.setOutputPath(job2,new Path(args[1]+(i+1)));
            job2.waitForCompletion(true);
        }

        Job job3 = Job.getInstance();
        job3.setJobName("MyJob3");
        job3.setJarByClass(Graph.class);
        job3.setOutputKeyClass(IntWritable.class);
        job3.setOutputValueClass(Tagged.class);
        job3.setMapOutputKeyClass(IntWritable.class);
        job3.setMapOutputValueClass(IntWritable.class);
        job3.setMapperClass(MyMapper3.class);
        job3.setInputFormatClass(SequenceFileInputFormat.class);
        job3.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job3,new Path(args[1]+iterations));
        FileOutputFormat.setOutputPath(job3,new Path(args[2]));
        job3.waitForCompletion(true);
    }
}
