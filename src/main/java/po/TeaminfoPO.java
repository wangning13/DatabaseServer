package po;

import java.io.Serializable;

public class TeaminfoPO implements Serializable{

	String name;//球队名字
	String abbr;//缩写
	String city;//所在城市
	String league;//所在联盟
	String partition;//所在分区
	String court;//球场
	int year;//创建时间
	public TeaminfoPO(String name, String abbr, String city, String league,
			String partition, String court, int year) {
		super();
		this.name = name;
		this.abbr = abbr;
		this.city = city;
		this.league = league;
		this.partition = partition;
		this.court = court;
		this.year = year;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getAbbr() {
		return abbr;
	}
	public void setAbbr(String abbr) {
		this.abbr = abbr;
	}
	public String getCity() {
		return city;
	}
	public void setCity(String city) {
		this.city = city;
	}
	public String getLeague() {
		return league;
	}
	public void setLeague(String league) {
		this.league = league;
	}
	public String getPartition() {
		return partition;
	}
	public void setPartition(String partition) {
		this.partition = partition;
	}
	public String getCourt() {
		return court;
	}
	public void setCourt(String court) {
		this.court = court;
	}
	public int getYear() {
		return year;
	}
	public void setYear(int year) {
		this.year = year;
	}
	
}
