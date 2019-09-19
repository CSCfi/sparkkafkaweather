package fi.csc.spark.weather.utils;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Instant;

import org.apache.spark.api.java.JavaRDD;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class SparkWeatherUtils {

	
	
public static Double getAvgTemperature(String json_message) {
		
		double avg_temperature = 0.0;
		double temperature_accumulator = 0.0;
		
		JsonParser parser = new JsonParser();
		JsonObject obj = (JsonObject) parser.parse(json_message);
		JsonArray arr = (JsonArray)obj.get("body");
		
		for(JsonElement station : arr) { // loop over all weather stations
			
			JsonObject station_obj = station.getAsJsonObject();
			
			JsonObject measures_obj = station_obj.get("measures").getAsJsonObject();
			
			String module_key = (String) measures_obj.keySet().toArray()[0];
			
			JsonObject res_obj = measures_obj.get(module_key).getAsJsonObject().get("res").getAsJsonObject();
			String res_key = (String) res_obj.keySet().toArray()[0];
			
			JsonArray res_station_id_array = res_obj.get(res_key).getAsJsonArray();
			
			temperature_accumulator += res_station_id_array.get(0).getAsDouble();	
			
		}
		
		avg_temperature = temperature_accumulator / arr.size();
		
		return avg_temperature;
		
	}
	
	

public static void appendAvgTemperatureToFile(JavaRDD<Double> rdd) throws IOException {
	
	if(rdd.collect().isEmpty())
		return;
	
	double temp = rdd.collect().get(0);
	
	BufferedWriter writer = new BufferedWriter(new FileWriter("/Users/hulkkone/temp/example.txt", true)); // true for append
	
	long now = Instant.now().getEpochSecond();
	String viz_data = String.format("%d,%f", now, temp);
	writer.newLine();
	writer.write(viz_data);
	
	writer.close();
	
}
	
	
				
}
