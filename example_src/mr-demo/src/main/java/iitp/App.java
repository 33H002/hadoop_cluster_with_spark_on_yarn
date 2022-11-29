package iitp;

import java.io.File;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Random;
import java.util.HashMap;
import java.util.Iterator;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.DoubleWritable;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;


public class App { 

    public static class TopBottomMapper extends Mapper<Object, Text, IntWritable, DoubleWritable>{

        String[] headerList, tmp;
        ArrayList<String> removeList = new ArrayList<String>();
        
        @Override
        protected void setup(Context context) throws IOException, InterruptedException { 

            JSONParser parser = new JSONParser();

            try {
                JSONObject jsonObject = (JSONObject) parser.parse(new InputStreamReader(new FileInputStream("columns.json"), "utf-8"));
                String header = (String) jsonObject.get("header");
                String remove = (String) jsonObject.get("remove");

                headerList = header.split(",");
                tmp = remove.split(",");

                for (int i = 0; i < tmp.length; i++) {
                    removeList.add(tmp[i]);
                }
                
            } catch (ParseException e1) {
                e1.printStackTrace();
            }
        }

        @Override
        protected void map(Object key, Text value, Mapper<Object, Text, IntWritable, DoubleWritable>.Context context) throws IOException, InterruptedException {

            String[] columns = value.toString().split(",");
            for (int i = 0; i < columns.length; i++) {
                if (!removeList.contains(headerList[i])) {
                    try {
                        Double outValue = Double.parseDouble(columns[i]);
                        context.write(new IntWritable(i), new DoubleWritable(outValue));
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                } 
            }
        }
    }

  
    public static class TopBottomReducer extends Reducer<IntWritable, DoubleWritable, IntWritable, Text> {
    
        @Override
        protected void reduce(IntWritable key, Iterable<DoubleWritable> values, Reducer<IntWritable, DoubleWritable, IntWritable, Text>.Context context) throws IOException, InterruptedException {
            
            int count = 0;
            double sum = 0;
            double sumOfSquares = 0;
            
            for (DoubleWritable value : values) {
                count += 1;
                sum += value.get();
                sumOfSquares += (value.get() * value.get());
            }

            double mean = sum / count;
            double sd = Math.sqrt((sumOfSquares / (count)) - mean*mean);
            double upper = mean + 3*sd;
            double lower = mean - 3*sd;

            context.write(key, new Text(Double.toString(upper) + "," + Double.toString(lower)));     
        } 
    }

    public static class MaskingMapper extends Mapper<Object, Text, IntWritable, Text>{	

        @Override
        protected void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            Random genRand = new Random();
            int rand = genRand.nextInt(40);
            context.write(new IntWritable(rand), value);
        }
                
    }

    public static class MaskingReducer extends Reducer<IntWritable, Text, NullWritable, Text>{

        String[] headerList, tmp;
        ArrayList<String> removeList = new ArrayList<String>();

        static HashMap<Integer, Double> upperMap = new HashMap<Integer, Double>();
        static HashMap<Integer, Double> lowerMap = new HashMap<Integer, Double>();
        
        @Override
        protected void setup(Context context) throws IOException, InterruptedException { 

            JSONParser parser = new JSONParser();
            try {
                JSONObject jsonObject = (JSONObject) parser.parse(new InputStreamReader(new FileInputStream("columns.json"), "utf-8"));
                String header = (String) jsonObject.get("header");
                String remove = (String) jsonObject.get("remove");

                headerList = header.split(",");
                tmp = remove.split(",");

                for (int i = 0; i < tmp.length; i++) {
                    removeList.add(tmp[i]);
                }
                
            } catch (ParseException e1) {
                e1.printStackTrace();
            }

            final File[] folder = new File("tmp").listFiles();
            for (final File fileEntry : folder) {
                if(!fileEntry.isDirectory()) {
                        String filename = "tmp/" + fileEntry.getName();
                        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream(filename), "utf-8"));
                        String line = null;
                        while( (line = bufferedReader.readLine()) != null)  {
                            String[] values = line.split("\\s+|,");
                            upperMap.put(Integer.valueOf(values[0]), Double.parseDouble(values[1]));
                            lowerMap.put(Integer.valueOf(values[0]), Double.parseDouble(values[2]));
                        }
                        bufferedReader.close();  
                }
            }
        }

        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) 
                throws IOException, InterruptedException {

            StringBuilder sb = new StringBuilder();
            Iterator<Text> iterator = values.iterator();

            while (iterator.hasNext()) {
                String[] value = iterator.next().toString().split(",");
                for(int i = 0; i < value.length; i++) {
                    String col = headerList[i];
                    if (!removeList.contains(col)) {
                        if (upperMap.containsKey(i)) {
                            if (upperMap.get(i) < Double.parseDouble(value[i])) {
                                sb.append(new Text(upperMap.get(i).toString()));   
                            } else if (lowerMap.get(i) > Double.parseDouble(value[i])) {
                                sb.append(new Text(lowerMap.get(i).toString()));  
                            } else {
                                sb.append(new Text(value[i].toString())); 
                            }
                        } else {
                            String val = value[i];
                            if(val.length()>2) { 
                                sb.append(new Text(val.substring(0, val.length()-2))+"**");
                            } else { 
                                sb.append(new Text(val)); 
                            }
                        }
                        if (i<value.length-1) sb.append(new Text(","));
                    }
                }
                sb.append(new Text("\n")); 
            }
            context.write(NullWritable.get(), new Text(sb.toString()));
        }
    }

    public static void main(String[] args) throws Exception { 

        Configuration conf = new Configuration();

        conf.set("mapreduce.input.fileinputformat.split.minsize", "268435456"); // 268435456, 536870912 // mapred.min.split.size
        conf.set("mapreduce.input.fileinputformat.split.maxsize", "268435456");

        conf.set("mapreduce.job.jvm.numtasks", "-1"); // JVM 재사용

        conf.set("mapreduce.tasktracker.map.tasks.maximum", "4");
        conf.set("mapreduce.tasktracker.reduce.tasks.maximum", "2");


        Job job1 = Job.getInstance(conf, "JOB_1");
        job1.addCacheFile(new URI(args[2]+"#columns.json"));
        job1.setJarByClass(App.class);
        job1.setMapperClass(TopBottomMapper.class);
        job1.setReducerClass(TopBottomReducer.class);
        job1.setMapOutputKeyClass(IntWritable.class);
        job1.setMapOutputValueClass(DoubleWritable.class);
        job1.setOutputKeyClass(IntWritable.class);
        job1.setOutputValueClass(Text.class);
                
        //Path tmp_output = new Path(args[1]+"-tmp");
        Path tmp_output = new Path("tmp");
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(tmp_output.toString()));

        FileSystem hdfs = FileSystem.get(conf);

        if (job1.waitForCompletion(true)) {
            Job job2 = Job.getInstance(conf, "JOB_2");    
            
            FileStatus[] fileStatus = hdfs.listStatus(tmp_output);
            for (FileStatus file : fileStatus) {  
                String[] filenames = file.getPath().toString().split("/");
                String filename = "tmp/" + filenames[filenames.length - 1];   
                job2.addCacheFile(new URI(filename+"#"+filename));          
            }
            
            job2.addCacheFile(new URI("tmp#tmp"));
            job2.addCacheFile(new URI(args[2]+"#columns.json"));
            job2.setJarByClass(App.class);
            job2.setMapperClass(MaskingMapper.class);
            job2.setReducerClass(MaskingReducer.class);
            job2.setMapOutputKeyClass(IntWritable.class);
            job2.setMapOutputValueClass(Text.class);
            job2.setOutputKeyClass(NullWritable.class);
            job2.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job2, new Path(args[0]));
            FileOutputFormat.setOutputPath(job2, new Path(args[1]));
            FileInputFormat.setInputDirRecursive(job2, true);

            System.exit(job2.waitForCompletion(true) ? 0 : 1);
            
        }

    }

}