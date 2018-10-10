package org.apache.hadoop.examples;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.List;
import java.util.Objects;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.util.GenericOptionsParser;

class Point implements Comparable<Point> {
    public float x;
    public float y;

    public Point(float x, float y) {
        this.x = x;
        this.y = y;
    }

    public String toString() {
        return "(" + this.x + "," + this.y + ")";
    }

    public int compareTo(Point other) {
        return Float.compare(this.x, other.x);
    }


}


public class SpatialJoin {

    public static class PointMapper
            extends Mapper<LongWritable, Text, Text, Text> {
        //filtered by the winddow
        private int window_x1;
        private int window_x2;
        private int window_y1;
        private int window_y2;
        //we partition the points by its x, dividing into 10 groups

        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {

            String[] data = value.toString().split(",");
            float x = Float.parseFloat(data[0]);
            float y = Float.parseFloat(data[1]);
            if (window_x1 <= x && window_x2 >= x && window_y1 <= y && window_y2 >= y) {
                if (x <= 1000) context.write(new Text("0"), value);
                if (1000 < x && x <= 2000) context.write(new Text("1"), value);
                if (2000 < x && x <= 3000) context.write(new Text("2"), value);
                if (3000 < x && x <= 4000) context.write(new Text("3"), value);
                if (4000 < x && x <= 5000) context.write(new Text("4"), value);
                if (5000 < x && x <= 6000) context.write(new Text("5"), value);
                if (6000 < x && x <= 7000) context.write(new Text("6"), value);
                if (7000 < x && x <= 8000) context.write(new Text("7"), value);
                if (8000 < x && x <= 9000) context.write(new Text("8"), value);
                if (9000 < x && x <= 10000) context.write(new Text("9"), value);
            }



        }

        public void setup(Context context) throws IOException, InterruptedException {
            window_x1 = Integer.parseInt(context.getConfiguration().get("x1"));
            window_x2 = Integer.parseInt(context.getConfiguration().get("x2"));
            window_y1 = Integer.parseInt(context.getConfiguration().get("y1"));
            window_y2 = Integer.parseInt(context.getConfiguration().get("y2"));

        }
    }

    public static class RectMapper
            extends Mapper<LongWritable, Text, Text, Text> {


        private int window_x1;
        private int window_x2;
        private int window_y1;
        private int window_y2;
        //filtered by the window
        //for rectangles on the cut lines, we put into both partitioners

        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] data = value.toString().split(",");
            float x = Float.parseFloat(data[1]);
            float y = Float.parseFloat(data[2]);
            float height = Float.parseFloat(data[3]);
            float width = Float.parseFloat(data[4]);
            Point p1 = new Point(x, y);
            Point p2 = new Point(x + width, y + height);
            Point p3 = new Point(x, y + height);
            Point p4 = new Point(x + width, y);
            if ((window_x1 <= p1.x && window_x2 >= p1.x && window_y1 <= p1.y && window_y2 >= p1.y) || (window_x1 <= p2.x && window_x2 >= p2.x && window_y1 <= p2.y && window_y2 >= p2.y) || (window_x1 <= p3.x && window_x2 >= p3.x && window_y1 <= p3.y && window_y2 >= p3.y) || (window_x1 <= p4.x && window_x2 >= p4.x && window_y1 <= p4.y && window_y2 >= p4.y)) {

                if (x <= 995) context.write(new Text(data[0]), new Text("0," + value.toString()));
                if (995 < x && x <= 1000) {
                    //float width = Float.parseFloat(data[4]);
                    if ((width + x) <= 1000) context.write(new Text(data[0]), new Text("0," + value.toString()));
                    else {
                        context.write(new Text(data[0]), new Text("0," + value.toString()));
                        context.write(new Text(data[0]), new Text("1," + value.toString()));
                    }
                }
                if (1000 < x && x <= 1995) context.write(new Text(data[0]), new Text("1," + value.toString()));
                if (1995 < x && x <= 2000) {
                    //float width = Float.parseFloat(data[4]);
                    if ((width + x) <= 2000) context.write(new Text(data[0]), new Text("1," + value.toString()));
                    else {
                        context.write(new Text(data[0]), new Text("1," + value.toString()));
                        context.write(new Text(data[0]), new Text("2," + value.toString()));
                    }
                }
                if (2000 < x && x <= 2995) context.write(new Text(data[0]), new Text("2," + value.toString()));
                if (2995 < x && x <= 3000) {
                    //float width = Float.parseFloat(data[4]);
                    if ((width + x) <= 3000) context.write(new Text(data[0]), new Text("2," + value.toString()));
                    else {
                        context.write(new Text(data[0]), new Text("2," + value.toString()));
                        context.write(new Text(data[0]), new Text("3," + value.toString()));
                    }
                }
                if (3000 < x && x <= 3995) context.write(new Text(data[0]), new Text("3," + value.toString()));
                if (3995 < x && x <= 4000) {
                    //float width = Float.parseFloat(data[4]);
                    if ((width + x) <= 4000) context.write(new Text(data[0]), new Text("3," + value.toString()));
                    else {
                        context.write(new Text(data[0]), new Text("3," + value.toString()));
                        context.write(new Text(data[0]), new Text("4," + value.toString()));
                    }
                }
                if (4000 < x && x <= 4995) context.write(new Text(data[0]), new Text("4," + value.toString()));
                if (4995 < x && x <= 5000) {
                    //float width = Float.parseFloat(data[4]);
                    if ((width + x) <= 5000) context.write(new Text(data[0]), new Text("4," + value.toString()));
                    else {
                        context.write(new Text(data[0]), new Text("4," + value.toString()));
                        context.write(new Text(data[0]), new Text("5," + value.toString()));
                    }
                }
                if (5000 < x && x <= 5995) context.write(new Text(data[0]), new Text("5," + value.toString()));
                if (5995 < x && x <= 6000) {
                    //float width = Float.parseFloat(data[4]);
                    if ((width + x) <= 6000) context.write(new Text(data[0]), new Text("5," + value.toString()));
                    else {
                        context.write(new Text(data[0]), new Text("5," + value.toString()));
                        context.write(new Text(data[0]), new Text("6," + value.toString()));
                    }
                }
                if (6000 < x && x <= 6995) context.write(new Text(data[0]), new Text("6," + value.toString()));
                if (6995 < x && x <= 7000) {
                    //float width = Float.parseFloat(data[4]);
                    if ((width + x) <= 7000) context.write(new Text(data[0]), new Text("6," + value.toString()));
                    else {
                        context.write(new Text(data[0]), new Text("6," + value.toString()));
                        context.write(new Text(data[0]), new Text("7," + value.toString()));
                    }
                }
                if (7000 < x && x <= 7995) context.write(new Text(data[0]), new Text("7," + value.toString()));
                if (7995 < x && x <= 7000) {
                    //float width = Float.parseFloat(data[4]);
                    if ((width + x) <= 7000) context.write(new Text(data[0]), new Text("7," + value.toString()));
                    else {
                        context.write(new Text(data[0]), new Text("7," + value.toString()));
                        context.write(new Text(data[0]), new Text("8," + value.toString()));
                    }
                }
                if (8000 < x && x <= 8995) context.write(new Text(data[0]), new Text("8," + value.toString()));
                if (8995 < x && x <= 9000) {
                    //float width = Float.parseFloat(data[4]);
                    if ((width + x) <= 4000) context.write(new Text(data[0]), new Text("8," + value.toString()));
                    else {
                        context.write(new Text(data[0]), new Text("8," + value.toString()));
                        context.write(new Text(data[0]), new Text("9," + value.toString()));
                    }
                }
                if (9000 < x && x <= 10000) context.write(new Text(data[0]), new Text("9," + value.toString()));
            }
        }

        public void setup(Context context) throws IOException, InterruptedException {
            window_x1 = Integer.parseInt(context.getConfiguration().get("x1"));
            window_x2 = Integer.parseInt(context.getConfiguration().get("x2"));
            window_y1 = Integer.parseInt(context.getConfiguration().get("y1"));
            window_y2 = Integer.parseInt(context.getConfiguration().get("y2"));

        }
    }

    public static class RegionPartitioner extends Partitioner<Text, Text> {
        public int getPartition(Text key, Text value, int numReduceTasks) {

            String _key = key.toString();
            if (_key.equals("0")) return 0;
            else if (_key.equals("1")) return 1 % numReduceTasks;
            else if (_key.equals("2")) return 2 % numReduceTasks;
            else if (_key.equals("3")) return 3 % numReduceTasks;
            else if (_key.equals("4")) return 4 % numReduceTasks;
            else if (_key.equals("5")) return 5 % numReduceTasks;
            else if (_key.equals("6")) return 6 % numReduceTasks;
            else if (_key.equals("7")) return 7 % numReduceTasks;
            else if (_key.equals("8")) return 8 % numReduceTasks;
            else if (_key.equals("9")) return 9 % numReduceTasks;
            else {
                String rect_num = value.toString().split(",")[0];
                switch (rect_num) {
                    case "0":
                        return 0;
                    case "1":
                        return 1 % numReduceTasks;
                    case "2":
                        return 2 % numReduceTasks;
                    case "3":
                        return 3 % numReduceTasks;
                    case "4":
                        return 4 % numReduceTasks;
                    case "5":
                        return 5 % numReduceTasks;
                    case "6":
                        return 6 % numReduceTasks;
                    case "7":
                        return 7 % numReduceTasks;
                    case "8":
                        return 8 % numReduceTasks;
                    case "9":
                        return 9 % numReduceTasks;

                }
                return 0;
            }

        }

    }

    public static class JoinReducer extends Reducer<Text, Text, Text, Text> {

        private List<Point> points = new ArrayList<Point>();
        private boolean flag = true;
        //we perform two binary searchs to find the index less than the rectangle & larger than the rectangle

        public int leftBinarySearch(List<Point> point, float x) {
            int l = 0, r = point.size() - 1;
            while (l <= r) {
                int m = l + (r - l) / 2;
                if (m == point.size() - 1) return m - 1;
                if ((point.get(m).x < x) && (m + 2 <= point.size()) && (point.get(m + 1).x >= x))
                    return m;
                if (point.get(m).x < x)
                    l = m + 1;
                else
                    r = m - 1;
            }
            return 0;
        }

        public int rightBinarySearch(List<Point> point, float x) {
            int l = 0, r = point.size() - 1;
            while (l <= r) {
                int m = l + (r - l) / 2;
                if (m == 0) return 1;
                if ((point.get(m).x > x) && (m > 0) && (point.get(m - 1).x <= x))
                    return m;
                if (point.get(m).x > x)
                    r = m - 1;
                else
                    l = m + 1;
            }
            return point.size();
        }

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //List<String> list = new ArrayList<String>();

            for (Text value : values) {
                if (key.toString().matches("\\d")) {
                    String[] str = value.toString().split(",");
                    float x = Float.parseFloat(str[0]);
                    float y = Float.parseFloat(str[1]);
                    Point new_point = new Point(x, y);
                    points.add(new_point);
                    //context.write(key, value);

                } else {
                    if (flag) {
                        Collections.sort(points);
                        flag = false;
                    }
                    //context.write(key, value);
                    String[] data = value.toString().split(",");
                    float x = Float.parseFloat(data[2]);
                    float y = Float.parseFloat(data[3]);
                    float height = Float.parseFloat(data[4]);
                    float width = Float.parseFloat(data[5]);
                    int start_index = leftBinarySearch(points, x);
                    int end_index = rightBinarySearch(points, x + width);
                    //if(start_index==-1) start_index = 0;
                    //if(end_index==-1) end_index = points.size()-1;

                    for (int i = start_index; i < end_index; i++) {
                        Point p = points.get(i);
                        //context.write(key, new Text(str[1]));
                        //float point_x = x_list.get(i);
                        //float point_y = y_list.get(i);
                        if ((p.y >= y) && (p.x >= x) && (p.y <= y + height) && (p.x <= x + width))
                            context.write(key, new Text("(" + String.valueOf(p.x) + "," + String.valueOf(p.y) + ")"));
                    }

                }


            }
            //context.write(key, new Text(String.valueOf(count)+","+String.valueOf(min)+","+String.valueOf(max)));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapred.textoutputformat.separator", ",");
        if (args.length != 3 && args.length != 7) {
            System.err.println("Usage: <HDFS Points Input File> <HDFS Rectangle Input File> <HDFS Output File>\n Or <HDFS Points Input File> <HDFS Rectangle Input File> <HDFS Output File> window.X1 window.Y1 window.X2 window.Y2");
            System.exit(2);
        }
        if (args.length == 7) {
            int window_x1 = Integer.parseInt(args[3]);
            int window_y1 = Integer.parseInt(args[4]);
            int window_x2 = Integer.parseInt(args[5]);
            int window_y2 = Integer.parseInt(args[6]);
            if (window_x1 >= 1 && window_x1 < window_x2 && window_x2 <= 10000 && window_y1 >= 1 && window_y1 <= window_y2 && window_y2 <= 10000) {
                conf.set("x1", args[3]);
                conf.set("y1", args[4]);
                conf.set("x2", args[5]);
                conf.set("y2", args[6]);
            } else {
                System.err.println("window not correct!");
                System.exit(2);
            }
        } else {
            conf.set("x1", "1");
            conf.set("x2", "10000");
            conf.set("y1", "1");
            conf.set("y2", "10000");
        }
        Job job = new Job(conf, "SpatialJoin");
        job.setJarByClass(SpatialJoin.class);
        //job.setCombinerClass(CustomerCombiner.class);
        job.setPartitionerClass(RegionPartitioner.class);
        job.setReducerClass(JoinReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setNumReduceTasks(10);
        job.setOutputValueClass(Text.class);
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, PointMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, RectMapper.class);

        //FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
