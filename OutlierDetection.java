package org.apache.hadoop.examples;

import java.io.IOException;
import java.lang.Math;
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


import java.util.Comparator;
import java.util.LinkedList;
//import java.util.List;


class Point {
    public float x;
    public float y;

    public Point(float x, float y) {
        this.x = x;
        this.y = y;
    }

    public String toString() {
        return "(" + this.x + "," + this.y + ")";
    }

    //public int compareTo(Point other){
    //    return Float.compare(this.x, other.x);
    //}
    public static float distanceSq(float x1, float y1,
                                   float x2, float y2) {
        x1 -= x2;
        y1 -= y2;
        return (float) (x1 * x1 + y1 * y1);
    }

}

class Node {

    public Point point;
    public Node left;
    public Node right;

    public Node() {
    }

    public Node(Point p) {
        this.point = p;

    }

    public float X() {
        return point.x;
    }

    public float Y() {
        return point.y;
    }

}

class KDTree {
    //Referred to https://github.com/kostyaev/2D-Tree
    private final Comparator<Node> compareX = new Comparator<Node>() {

        @Override
        public int compare(Node p1, Node p2) {
            return Float.compare(p1.X(), p2.X());
        }
    };
    private final Comparator<Node> compareY = new Comparator<Node>() {

        @Override
        public int compare(Node p1, Node p2) {
            return Float.compare(p1.Y(), p2.Y());
        }
    };
    private Node root;

    public KDTree(List<Node> nodes) {
        buildTree(nodes);
    }

    //build the KDTree by comparing the x and y
    public void buildTree(List<Node> nodes) {
        root = new Object() {
            Node buildTree(boolean divX, List<Node> nodes) {
                if (nodes == null || nodes.isEmpty())
                    return null;
                Collections.sort(nodes, divX ? compareX : compareY);
                int mid = nodes.size() / 2;
                Node node = new Node();
                node.point = nodes.get(mid).point;
                node.left = buildTree(!divX, nodes.subList(0, mid));
                if (mid + 2 <= nodes.size())
                    node.right = buildTree(!divX, nodes.subList(mid + 1, nodes.size()));
                return node;
            }
        }.buildTree(true, nodes);
    }


    public List<Point> rangeSearch(final float  x, final float y, final float dist, final int count) {
        return new Object() {
            List<Point> result = new LinkedList<Point>();
            float radius = dist * dist;
            List<Point> rangeSearch(Node node, boolean divX) {
                if (node == null)
                    return result;
                float dis = Point.distanceSq(node.X(), node.Y(), x, y);
                if (radius >= dis)
                    result.add(node.point);
                // when we find enough points, output the result
                if (result.size() >= count) {
                    return result;
                }
                float delta = divX ? x - node.X() : y - node.Y();
                float delta2 = delta * delta;
                Node node1 = delta < 0 ? node.left : node.right;
                Node node2 = delta < 0 ? node.right : node.left;
                rangeSearch(node1, !divX);
                if (delta2 < radius) {
                    rangeSearch(node2, !divX);
                }
                return result;
            }
        }.rangeSearch(root, true);
    }
}


public class OutlierDetection {

    public static class PointMapper
            extends Mapper<LongWritable, Text, Text, Text> {


        private int k;
        private float r;
        private Text country_code = new Text();
        // we put all points in [n*1000-r, (n+1)*1000+r] to nth partition
        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {
            //StringTokenizer itr = new StringTokenizer(value.toString(), ",");
            String[] data = value.toString().split(",");
            float x = Float.parseFloat(data[0]);
            float y = Float.parseFloat(data[1]);

            if (x <= 1000 + r) context.write(new Text("0"), value);
            if (1000 - r <= x && x <= 2000 + r) context.write(new Text("1"), value);
            if (2000 - r <= x && x <= 3000 + r) context.write(new Text("2"), value);
            if (3000 - r <= x && x <= 4000 + r) context.write(new Text("3"), value);
            if (4000 - r <= x && x <= 5000 + r) context.write(new Text("4"), value);
            if (5000 - r <= x && x <= 6000 + r) context.write(new Text("5"), value);
            if (6000 - r <= x && x <= 7000 + r) context.write(new Text("6"), value);
            if (7000 - r <= x && x <= 8000 + r) context.write(new Text("7"), value);
            if (8000 - r <= x && x <= 9000 + r) context.write(new Text("8"), value);
            if (9000 - r <= x && x <= 10000) context.write(new Text("9"), value);



        }

        public void setup(Context context) throws IOException, InterruptedException {
            k = Integer.parseInt(context.getConfiguration().get("k"));
            r = Float.parseFloat(context.getConfiguration().get("r"));
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
            else return 0;


        }

    }

    public static class DetectionReducer extends Reducer<Text, Text, Text, Text> {

        private List<Node> nodes = new ArrayList<Node>();
        private List<Integer> outliers = new ArrayList<Integer>();
        private float r;
        private int k;


        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //List<String> list = new ArrayList<String>();

            for (Text value : values) {
                if (key.toString().matches("\\d")) {
                    String[] str = value.toString().split(",");
                    float x = Float.parseFloat(str[0]);
                    float y = Float.parseFloat(str[1]);
                    Node node = new Node(new Point(x, y));
                    nodes.add(node);
                    //context.write(key, value);
                }

            }

            KDTree kdtree = new KDTree(nodes);
            List<Point> result = new ArrayList<Point>();
            for (int i = 0; i < nodes.size(); i++) {
                Point x = nodes.get(i).point;
                result = kdtree.rangeSearch(x.x, x.y, r, k);
                if (result.size() < k) {
                    outliers.add(i);
                }
            }

        }

        public void setup(Context context) throws IOException, InterruptedException {
            k = Integer.parseInt(context.getConfiguration().get("k"));
            r = Float.parseFloat(context.getConfiguration().get("r"));
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            //when we output the result, we only output the point in [n*1000,n+1*1000]
            int partition_num = Integer.valueOf(context.getConfiguration().get("mapred.task.partition"));
            for (int i = 0; i < outliers.size(); i++) {
                Point p = nodes.get(outliers.get(i)).point;
                int left_boundary = partition_num * 1000;
                int right_boundary = (partition_num + 1) * 1000;
                if ((p.x >= left_boundary) && (p.x <= right_boundary))
                    context.write(new Text(String.valueOf(p.x)), new Text(String.valueOf(p.y)));
            }
        }


    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapred.textoutputformat.separator", ",");
        if (args.length != 4) {
            System.err.println("Error, Usage: <HDFS Points Input File> <HDFS Output File> r k");
            System.exit(2);
        }
        int k = Integer.parseInt(args[3]);
        float r = Float.parseFloat(args[2]);


        conf.set("r", args[2]);
        conf.set("k", args[3]);
        Job job = new Job(conf, "OutlierDetection");
        job.setJarByClass(OutlierDetection.class);
        //job.setCombinerClass(CustomerCombiner.class);
        job.setPartitionerClass(RegionPartitioner.class);
        job.setReducerClass(DetectionReducer.class);
        job.setMapperClass(PointMapper.class);
        //job.setCombinerClass(IntSumReducer.class);
        //job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setNumReduceTasks(10);
        job.setOutputValueClass(Text.class);
        //MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, PointMapper.class);
        //MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, RectMapper.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
