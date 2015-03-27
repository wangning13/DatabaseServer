package po;

import java.io.Serializable;

public class TeamMatchPO implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	String date;
	String hostGuest;
	String name;
	String opponent;
	String winLose;
	int total;
	int first;
	int second;
	int third;
	int fourth;
	public TeamMatchPO(String date, String hostGuest, String name,
			String opponent, String winLose, int total, int first, int second,
			int third, int fourth) {
		super();
		this.date = date;
		this.hostGuest = hostGuest;
		this.name = name;
		this.opponent = opponent;
		this.winLose = winLose;
		this.total = total;
		this.first = first;
		this.second = second;
		this.third = third;
		this.fourth = fourth;
	}
	public String getDate() {
		return date;
	}
	public void setDate(String date) {
		this.date = date;
	}
	public String getHostGuest() {
		return hostGuest;
	}
	public void setHostGuest(String hostGuest) {
		this.hostGuest = hostGuest;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getOpponent() {
		return opponent;
	}
	public void setOpponent(String opponent) {
		this.opponent = opponent;
	}
	public String getWinLose() {
		return winLose;
	}
	public void setWinLose(String winLose) {
		this.winLose = winLose;
	}
	public int getTotal() {
		return total;
	}
	public void setTotal(int total) {
		this.total = total;
	}
	public int getFirst() {
		return first;
	}
	public void setFirst(int first) {
		this.first = first;
	}
	public int getSecond() {
		return second;
	}
	public void setSecond(int second) {
		this.second = second;
	}
	public int getThird() {
		return third;
	}
	public void setThird(int third) {
		this.third = third;
	}
	public int getFourth() {
		return fourth;
	}
	public void setFourth(int fourth) {
		this.fourth = fourth;
	}
	
}
