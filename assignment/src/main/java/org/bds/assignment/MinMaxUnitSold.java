package org.bds.assignment;

import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.bds.pojo.SalesSoldUnits;

/*
 * 3.	Find the max and min units_sold in any order for each year by country for a given item type. 
 * 		Use a custom partitioner class instead of default hash based. 
 */
public class MinMaxUnitSold {

	public static String country = "";
	public static String itemType = "";
	public static String year = "";
	public static int count = 0;
	public static double tAvgUnitPrice = 0.0;

	/*
	 * Mapper Class to identify the records based on given inputs such as
	 * country , item type and year and pass to reducer
	 */

	public static class MinMaxMapper extends Mapper<Object, Text, Text, Text> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		final String DELIMITER = ",";
		Double unitPrice = 0.0;
		String tUniquekey = "";

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			try {

				if (value.toString().contains("country")) {

					return;
				} else {

					String[] sales = value.toString().split(DELIMITER);
					String countryName = sales[2].toLowerCase();
					String tItemType = sales[3].toLowerCase();
					String orderDate = sales[6];
					String tSoldUnit = sales[9];
					Double unitSold = Double.parseDouble(tSoldUnit);
					DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
					LocalDate localDate = LocalDate.parse(orderDate, formatter);
					int lYear = localDate.getYear();
					tUniquekey = countryName + "-" + tItemType;
					String givenValues = country + "-" + itemType;

					if (tUniquekey.equalsIgnoreCase(givenValues)) {

						word.set(tUniquekey);
						context.write(word, new Text(lYear + "=" + unitSold));

					}

				}

			} catch (Exception e) {
				e.printStackTrace();
			}
		}

	}
	/*
	 * Reducer Class to calculate the Min Max value based on the input from Mapper
	 */
	public static class MinMaxReducer extends Reducer<Text, Text, Text, Text> {

		public static List<SalesSoldUnits> sortedList = new ArrayList<SalesSoldUnits>();

		public void reduce(Text term, Iterable<Text> unitsSold, Context context)
				throws IOException, InterruptedException {

			for (Text val : unitsSold) {

				SalesSoldUnits salesSoldUnits = new SalesSoldUnits();
				salesSoldUnits.setYear(val.toString().split("=")[0]);
				salesSoldUnits.setUnitsSold((Double.valueOf(val.toString().split("=")[1])));

				this.sortedList.add(salesSoldUnits);

			}

			Collections.sort(this.sortedList, Collections.reverseOrder());

			for (SalesSoldUnits lis : this.sortedList) {

			}

			Map<String, Double> groupedMap = new TreeMap<String, Double>();
			for (int i = 0; i < sortedList.size(); i++) {

				if ((groupedMap.get(sortedList.get(i).getYear() + "_MIN")) != null) {
					if (sortedList.get(i).getUnitsSold() < (groupedMap.get(sortedList.get(i).getYear() + "_MIN"))) {
						groupedMap.put(sortedList.get(i).getYear() + "_MIN", sortedList.get(i).getUnitsSold());
					}
				} else {
					groupedMap.put(sortedList.get(i).getYear() + "_MIN", sortedList.get(i).getUnitsSold());
				}
				if ((groupedMap.get(sortedList.get(i).getYear() + "_MAX")) != null) {
					if (sortedList.get(i).getUnitsSold() > (groupedMap.get(sortedList.get(i).getYear() + "_MAX"))) {
						groupedMap.put(sortedList.get(i).getYear() + "_MAX", sortedList.get(i).getUnitsSold());
					}
				} else {
					groupedMap.put(sortedList.get(i).getYear() + "_MAX", sortedList.get(i).getUnitsSold());
				}

			}

			for (Map.Entry<String, Double> entry : groupedMap.entrySet()) {

				System.out.println("Year :: " + entry.getKey() + " SoldUnit:: " + entry.getValue());
			}
			Text output = new Text(this.sortedList.toString());
			context.write(term, output);
		}

	}
	/*
	 * Partitioner Class to partition based on the year 
	 */
	public static class MinMaxPartitioner extends Partitioner<Text, Text> {

		@Override
		public int getPartition(Text key, Text value, int numReduceTasks) {
			String[] str = value.toString().split("=");

			int year = Integer.parseInt(str[0]);

			if (numReduceTasks == 0) {
				return 0;
			}

			if (year <= 2015) {
				return 0;
			} else if (year > 2015 && year <= 2020) {
				return 1 % numReduceTasks;
			} else {
				return 2 % numReduceTasks;
			}
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		if (otherArgs.length != 4) {
			System.err.println("* * * pls enter valid arguments as mentioned in document");
			System.exit(2);
		}

		Job job = Job.getInstance(conf, "Avg Unit Price");

		job.setPartitionerClass(MinMaxPartitioner.class);
		job.setJarByClass(MinMaxUnitSold.class);
		job.setMapperClass(MinMaxMapper.class);
		job.setReducerClass(MinMaxReducer.class);
		job.setNumReduceTasks(10);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		country = args[0].toLowerCase();
		itemType = args[1].toLowerCase();

		FileInputFormat.addInputPath(job, new Path(otherArgs[2]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[3] + Math.random()));

		boolean status = job.waitForCompletion(true);

		if (status) {
			System.exit(0); // Exit with Success code
			System.out.println("Completed the Map Reduce Functions Successfully");
		} else {
			System.exit(1); // Exit with Failure code # 1
			System.out.println("exiting with failure");
		}

	}

}
