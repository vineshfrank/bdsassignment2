package org.bds.assignment;

import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.bds.pojo.OrderProfit;

/*
 * 	4.	What are the top 10 order id for a given year by the total_profit 
 */
public class TopOrders {

	public static String country = "";
	public static String itemType = "";
	public static String year = "";
	public static int count = 0;
	public static double totalProfits = 0.0;

	/*
	 * Mapper Class to identify the records based on given inputs such as year
	 * and pass to reducer
	 */
	public static class TopOrdersMapper extends Mapper<Object, Text, Text, Text> {

		private Text word = new Text();
		final String DELIMITER = ",";
		Double unitPrice = 0.0;
		String tUniquekey = "";
		List index_Sales = new ArrayList<Integer>();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			try {
				if (value.toString().contains("country")) {
					return;
				} else {
					String[] sales = value.toString().split(DELIMITER);

					index_Sales.add(sales[0]);
					String countryName = sales[2].toLowerCase();
					String orderId = sales[7];
					String orderDate = sales[6];
					String profit = sales[14];
					Double salesProfit = Double.parseDouble(profit);
					DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
					LocalDate localDate = LocalDate.parse(orderDate, formatter);
					int salesYear = localDate.getYear();

					if (salesYear == Integer.valueOf(year)) {

						word.set(orderId);
						context.write(word, new Text(orderId + "=" + salesProfit));

					}

				}

			} catch (Exception e) {
				e.printStackTrace();
			}
		}

	}

	/*
	 * Reducer Class to calculate the average based on the input from Mapper
	 */
	public static class TopOrdersReducer extends Reducer<Text, Text, Text, Text> {

		public static List<OrderProfit> sortedList = new ArrayList<OrderProfit>();

		public void reduce(Text term, Iterable<Text> orderProfit, Context context)
				throws IOException, InterruptedException {
			for (Text val : orderProfit) {

				OrderProfit orderProfit1 = new OrderProfit();
				orderProfit1.setOrderId(val.toString().split("=")[0]);
				orderProfit1.setProfit(Double.valueOf(val.toString().split("=")[1]));

				this.sortedList.add(orderProfit1);

			}

			Collections.sort(this.sortedList, Collections.reverseOrder());

			Text output = new Text(this.sortedList.toString());
			context.write(term, output);
		}

	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		if (otherArgs.length != 3) {
			System.err.println("* * * Needs Three arguments....usage : AvgUnitPrice <input_file> <output_folder>");
			System.exit(2);
		}

		Job job = Job.getInstance(conf, "Top 10 Orders");
		job.setJarByClass(TopOrders.class);
		job.setMapperClass(TopOrdersMapper.class);
		job.setReducerClass(TopOrdersReducer.class);
		job.setNumReduceTasks(10);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		year = args[0];
		if (year == null || year.length() < 4) {
			System.out.println("Please enter a valid year");
		}
		FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2] + Math.random()));

		boolean status = job.waitForCompletion(true);
		System.out.println("#################################### \n");
		System.out.println("TOP 10 Orders based on Profit in Year : " + year + "\n");
		for (int i = 0; i < 9; i++) {
			System.out.println("Order ID : " + TopOrdersReducer.sortedList.get(i).getOrderId() + "  Profit : "
					+ TopOrdersReducer.sortedList.get(i).getProfit());

		}
		if (status) {

			System.exit(0); // Exit with Success code
			System.out.println("Completed the Map Reduce Functionalities Successfully");
		} else {
			System.exit(1); // Exit with Failure code # 1
			System.out.println("exiting with failure");
		}

	}

}
