package com.utdallas.bigdata.assignment;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.utdallas.bigdata.assignment.UserKeyWritable;
import com.utdallas.bigdata.assignment.FriendsWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
public class MutualFriends {
	public static class Map extends Mapper<LongWritable, Text, UserKeyWritable, FriendsWritable>{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String input[] = value.toString().split("\t");
			long userId = Long.parseLong(input[0]);
			if(input.length > 1){
				String friendsList[] = input[1].split(",");
				for(String friend:friendsList){
					UserKeyWritable userKey = new UserKeyWritable();
					FriendsWritable userValue = new FriendsWritable();
					userKey.set(userId, Long.parseLong(friend));
					userValue.set(friendsList);
					context.write(userKey,userValue);
				}
			}
		}
	}
	
	public static class Reduce extends Reducer<UserKeyWritable,FriendsWritable,Text,Text> {
		private Text outputKey = new Text();
		private Text outputValue = new Text();
		public void reduce(UserKeyWritable key, Iterable<FriendsWritable> values, Context context) throws IOException, InterruptedException{
			int size = 0;
			List<String> users = new ArrayList<String>();
			for(FriendsWritable mutualFriends:values){
				users.add(mutualFriends.getFriends().toString());
				size++;
			}
			if(size == 2){
				String userFriends1 = users.get(0);
				String userFriends2 = users.get(1);
				List<String> mutualFriends = intersectionList(userFriends1, userFriends2);
				if(mutualFriends.size() > 0){
					outputValue.set(mutualFriends.toString());
					outputKey.set(key.getUserid1()+"\t"+key.getUserid2());
					context.write(outputKey, outputValue);
				}
			}
			users = null;
		}
		
		public List<String> intersectionList(String list1, String list2){
			list1 = list1.substring(1,list1.length()-1).replaceAll("[^0-9,]", "");
			list2 = list2.substring(1,list2.length()-1).replaceAll("[^0-9,]", "");
			List<String> userList1 = Arrays.asList(list1.split(","));
			List<String> userList2 = Arrays.asList(list2.split(","));
			List<String> intersection = new ArrayList<String>();
			for(String id:userList1){
				if(userList2.contains(id)){
					intersection.add(id);
				}
			}
			return intersection;
		}
	}

	public static void main(String[] args) throws Exception {
	  	Configuration conf = new Configuration();
	  	String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	  	if (otherArgs.length != 2) {
	  	System.err.println("Usage: Min_Max <in> <out>");
	  	System.exit(2);
	  	}
	  	Job job = new Job(conf, "MutualFriends");
	  	job.setJarByClass(MutualFriends.class);
	  	job.setMapperClass(Map.class);
	  	job.setReducerClass(Reduce.class);
	  	job.setOutputKeyClass(Text.class);
	  	job.setOutputValueClass(Text.class);
	  	job.setMapOutputKeyClass(UserKeyWritable.class);
	  	job.setMapOutputValueClass(FriendsWritable.class);
	  	FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
	  	FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
	  	System.exit(job.waitForCompletion(true) ? 0 : 1);
  	}


}
