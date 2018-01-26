package edu.uta.cse6331;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import java.io.IOException;
import java.util.Scanner;
import org.apache.hadoop.mapreduce.lib.output.*;
import java.io.IOException;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.conf.Configuration;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;

public class Multiply {

	public static class Mapr1 extends Mapper<LongWritable, Text, Text, Text> {
		@Override
		public void map(LongWritable ky, Text vl, Context cntxt) throws InterruptedException, IOException {

			String tvl = vl.toString();
			System.out.println(tvl);
			String[] sLst = tvl.split(",");
			System.out.println(sLst.length);

			int tg = 0;
			String myky = sLst[1];
			String myvl = tg + "," + sLst[0] + "," + sLst[2];
			System.out.println(myvl);
			cntxt.write(new Text(myky), new Text(myvl));
		}
	}

	public static class Mapr11 extends Mapper<LongWritable, Text, Text, Text> {
		@Override
		public void map(LongWritable ky, Text vl, Context cntxt) throws InterruptedException, IOException {

			String tvl = vl.toString();
			System.out.println(tvl);
			String[] sLst = tvl.split(",");
			System.out.println(sLst.length);

			int tg = 1;
			String myky = sLst[0];
			String myVl = tg + "," + sLst[1] + "," + sLst[2];
			System.out.println(myVl);
			cntxt.write(new Text(myky), new Text(myVl));

		}
	}

	public static class Redcr1 extends Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text ky, Iterable<Text> vl, Context cntxt) throws InterruptedException, IOException {
			HashMap<Integer, Float> mapX = new HashMap<Integer, Float>();
			HashMap<Integer, Float> mapY = new HashMap<Integer, Float>();
			for (Text myvl : vl) {
				String[] myvalue = myvl.toString().split(",");
				if (myvalue[0].equals("0")) {
					mapX.put(Integer.parseInt(myvalue[1]), Float.parseFloat(myvalue[2]));
				} else {
					mapY.put(Integer.parseInt(myvalue[1]), Float.parseFloat(myvalue[2]));
				}
			}

			for (Integer myval1 : mapX.keySet()) {
				for (Integer myval2 : mapY.keySet()) {
					float rslt = mapY.get(myval2) * mapX.get(myval1);
					String rslt2 = String.valueOf(rslt);
					cntxt.write(new Text(myval1 + "," + myval2), new Text(rslt2));
				}
			}

		}
	}

	public static class Mapr2 extends Mapper<LongWritable, Text, Text, Text> {
		@Override
		public void map(LongWritable ky, Text vl, Context cntxt) throws InterruptedException, IOException {

			String tvl = vl.toString();
			tvl = tvl.replaceAll("[\\t]", ",");
			String[] sLst = tvl.split(",");
			String myky = sLst[0] + "," + sLst[1];
			String myvl = sLst[2];
			cntxt.write(new Text(myky), new Text(myvl));
		}
	}

	public static class Redcr2 extends Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text ky, Iterable<Text> vl, Context cntxt) throws IOException, InterruptedException {

			String[] kys = ky.toString().split(",");
			String myvl1 = kys[0];
			String myvl2 = kys[1];

			float total = 0;
			for (Text myvl : vl) {

				String strn = myvl.toString();
				total += Float.parseFloat(strn);
			}
			String rslt = String.valueOf(total);
			String multiplky = myvl1 + "," + myvl2;
			String multiplvl = rslt;
			cntxt.write(new Text(multiplky), new Text(multiplvl));
		}
	}

	public static void main(String[] args) throws Exception {

		Job jb1 = Job.getInstance();
		jb1.setNumReduceTasks(2);
		jb1.setJobName("Job1");
		jb1.setJarByClass(Multiply.class);
		jb1.setReducerClass(Redcr1.class);
		jb1.setInputFormatClass(TextInputFormat.class);
		jb1.setOutputFormatClass(TextOutputFormat.class);
		jb1.setOutputKeyClass(Text.class);
		jb1.setOutputValueClass(Text.class);
		MultipleInputs.addInputPath(jb1, new Path(args[0]), TextInputFormat.class, Mapr1.class);
		MultipleInputs.addInputPath(jb1, new Path(args[1]), TextInputFormat.class, Mapr11.class);
		FileOutputFormat.setOutputPath(jb1, new Path(args[2]));

		jb1.waitForCompletion(true);

		Job jb2 = Job.getInstance();
		jb2.setJobName("Job2");
		jb2.setJarByClass(Multiply.class);
		jb2.setMapperClass(Mapr2.class);
		jb2.setReducerClass(Redcr2.class);
		jb2.setInputFormatClass(TextInputFormat.class);
		jb2.setOutputFormatClass(TextOutputFormat.class);
		jb2.setOutputKeyClass(Text.class);
		jb2.setOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(jb2, new Path(args[2]));
		FileOutputFormat.setOutputPath(jb2, new Path(args[3]));
		System.exit(jb2.waitForCompletion(true) ? 0 : 1);

	}
}
