package po;

public class PlayerMatchPO {

	String date;
	String team;
	String playername;
	String position;
	double minutes;
	int fieldGoal;
	int fieldGoalAttempts;
	int threepointFieldGoal;
	int threepointFieldGoalAttempts;
	int freeThrow;
	int freeThrowAttempts;
	int offensiveRebound;
	int defensiveRebound;
	int backboard;
	int assit;
	int steal;
	int block;
	int turnOver;
	int foul;
	int scoring;
	public PlayerMatchPO(String date, String team, String playername,
			String position, double minutes, int fieldGoal,
			int fieldGoalAttempts, int threepointFieldGoal,
			int threepointFieldGoalAttempts, int freeThrow,
			int freeThrowAttempts, int offensiveRebound, int defensiveRebound,
			int backboard, int assit, int steal, int block, int turnOver,
			int foul, int scoring) {
		super();
		this.date = date;
		this.team = team;
		this.playername = playername;
		this.position = position;
		this.minutes = minutes;
		this.fieldGoal = fieldGoal;
		this.fieldGoalAttempts = fieldGoalAttempts;
		this.threepointFieldGoal = threepointFieldGoal;
		this.threepointFieldGoalAttempts = threepointFieldGoalAttempts;
		this.freeThrow = freeThrow;
		this.freeThrowAttempts = freeThrowAttempts;
		this.offensiveRebound = offensiveRebound;
		this.defensiveRebound = defensiveRebound;
		this.backboard = backboard;
		this.assit = assit;
		this.steal = steal;
		this.block = block;
		this.turnOver = turnOver;
		this.foul = foul;
		this.scoring = scoring;
	}
	public String getDate() {
		return date;
	}
	public void setDate(String date) {
		this.date = date;
	}
	public String getTeam() {
		return team;
	}
	public void setTeam(String team) {
		this.team = team;
	}
	public String getPlayername() {
		return playername;
	}
	public void setPlayername(String playername) {
		this.playername = playername;
	}
	public String getPosition() {
		return position;
	}
	public void setPosition(String position) {
		this.position = position;
	}
	public double getMinutes() {
		return minutes;
	}
	public void setMinutes(double minutes) {
		this.minutes = minutes;
	}
	public int getFieldGoal() {
		return fieldGoal;
	}
	public void setFieldGoal(int fieldGoal) {
		this.fieldGoal = fieldGoal;
	}
	public int getFieldGoalAttempts() {
		return fieldGoalAttempts;
	}
	public void setFieldGoalAttempts(int fieldGoalAttempts) {
		this.fieldGoalAttempts = fieldGoalAttempts;
	}
	public int getThreepointFieldGoal() {
		return threepointFieldGoal;
	}
	public void setThreepointFieldGoal(int threepointFieldGoal) {
		this.threepointFieldGoal = threepointFieldGoal;
	}
	public int getThreepointFieldGoalAttempts() {
		return threepointFieldGoalAttempts;
	}
	public void setThreepointFieldGoalAttempts(int threepointFieldGoalAttempts) {
		this.threepointFieldGoalAttempts = threepointFieldGoalAttempts;
	}
	public int getFreeThrow() {
		return freeThrow;
	}
	public void setFreeThrow(int freeThrow) {
		this.freeThrow = freeThrow;
	}
	public int getFreeThrowAttempts() {
		return freeThrowAttempts;
	}
	public void setFreeThrowAttempts(int freeThrowAttempts) {
		this.freeThrowAttempts = freeThrowAttempts;
	}
	public int getOffensiveRebound() {
		return offensiveRebound;
	}
	public void setOffensiveRebound(int offensiveRebound) {
		this.offensiveRebound = offensiveRebound;
	}
	public int getDefensiveRebound() {
		return defensiveRebound;
	}
	public void setDefensiveRebound(int defensiveRebound) {
		this.defensiveRebound = defensiveRebound;
	}
	public int getBackboard() {
		return backboard;
	}
	public void setBackboard(int backboard) {
		this.backboard = backboard;
	}
	public int getAssit() {
		return assit;
	}
	public void setAssit(int assit) {
		this.assit = assit;
	}
	public int getSteal() {
		return steal;
	}
	public void setSteal(int steal) {
		this.steal = steal;
	}
	public int getBlock() {
		return block;
	}
	public void setBlock(int block) {
		this.block = block;
	}
	public int getTurnOver() {
		return turnOver;
	}
	public void setTurnOver(int turnOver) {
		this.turnOver = turnOver;
	}
	public int getFoul() {
		return foul;
	}
	public void setFoul(int foul) {
		this.foul = foul;
	}
	public int getScoring() {
		return scoring;
	}
	public void setScoring(int scoring) {
		this.scoring = scoring;
	}
	
}
