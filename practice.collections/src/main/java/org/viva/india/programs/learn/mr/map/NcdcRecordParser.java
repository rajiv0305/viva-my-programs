package org.viva.india.programs.learn.mr.map;

import org.apache.hadoop.io.Text;

public class NcdcRecordParser {

	private static final int MISSING_TEMPERATURE = 9999;
	
	private String year;
	private int airTemp;
	private String quality;
	
	public void parse(Text value){
		parse(value.toString());
	}
	
	public void parse(String record){
		year = record.substring(15, 19);
		String airTemperatureString;
		// Remove leading plus sign as parseInt doesn't like them (pre-Java 7)
		if (record.charAt(87) == '+') {
		airTemperatureString = record.substring(88, 92);
		} else {
		airTemperatureString = record.substring(87, 92);
		}
		airTemp = Integer.parseInt(airTemperatureString);
		quality = record.substring(92, 93);
	}
	
	public boolean isValidTemperature(){
		return airTemp != MISSING_TEMPERATURE && quality.matches("[01459]");
	}
	
	public String getYear() {
		return year;
	}
	public void setYear(String year) {
		this.year = year;
	}
	public int getAirTemp() {
		return airTemp;
	}
	public void setAirTemp(int airTemp) {
		this.airTemp = airTemp;
	}
	public String getQuality() {
		return quality;
	}
	public void setQuality(String quality) {
		this.quality = quality;
	}
	
	
}
