package po;

import java.io.Serializable;

public class TeamPO implements Serializable{
	int opponentFieldGoal;//对手投篮命中数
	int opponentFieldGoalAttempts;//对手投篮出手次数
	int opponentTurnOver;//对手失误数
	int opponentFreeThrowAttempts;//对手罚球数
	int oppenentScoring;//对手得分
	String teamName;//球队名称
	int matches;//比赛场数
	int wins;//胜利场数
	int fieldGoal;//投篮命中数
	int fieldGoalAttempts;//投篮出手次数
	int threePointFieldGoal;//三分命中数
	int threePointFieldGoalAttempts;//三分出手数
	int freeThrow;//罚球命中数
	int freeThrowAttempts;//罚球出手数
	int offensiveRebound;//进攻篮板数
	int defensiveRebound;//防守篮板数
	int opponentOffensiveRebound;//对手进攻篮板数
	int opponentDefensiveRebound;//对手防守篮板数
	int backboard;//篮板数
	int assist;//助攻数
	int steal;//抢断数
	int block;//盖帽数
	int turnOver;//失误数
	int foul;//犯规数
	int scoring;//比赛得分
	
	double fieldGoalPercentage;//投篮命中率
	double threePointShotPercentage;//三分命中率
	double freeThrowPercentage;//三分命中率
	double winningPercentage;//胜率
	double possessions;//进攻回合
	double offensiveEfficiency;//进攻效率
	double defensiveEfficiency;//防守效率
	double offensivebackboardEfficiency;//进攻篮板效率
	double defensivebackboardEfficiency;//防守篮板效率
	double stealEfficiency;//抢断效率
	double assitEfficiency;//助攻效率
	public TeamPO(int opponentFieldGoal, int opponentFieldGoalAttempts,
			int opponentTurnOver, int opponentFreeThrowAttempts,
			int oppenentScoring, String teamName, int matches, int wins,
			int fieldGoal, int fieldGoalAttempts, int threePointFieldGoal,
			int threePointFieldGoalAttempts, int freeThrow,
			int freeThrowAttempts, int offensiveRebound, int defensiveRebound,
			int opponentOffensiveRebound, int opponentDefensiveRebound,
			int backboard, int assist, int steal, int block, int turnOver,
			int foul, int scoring, double fieldGoalPercentage,
			double threePointShotPercentage, double freeThrowPercentage,
			double winningPercentage, double possessions,
			double offensiveEfficiency, double defensiveEfficiency,
			double offensivebackboardEfficiency,
			double defensivebackboardEfficiency, double stealEfficiency,
			double assitEfficiency) {
		super();
		this.opponentFieldGoal = opponentFieldGoal;
		this.opponentFieldGoalAttempts = opponentFieldGoalAttempts;
		this.opponentTurnOver = opponentTurnOver;
		this.opponentFreeThrowAttempts = opponentFreeThrowAttempts;
		this.oppenentScoring = oppenentScoring;
		this.teamName = teamName;
		this.matches = matches;
		this.wins = wins;
		this.fieldGoal = fieldGoal;
		this.fieldGoalAttempts = fieldGoalAttempts;
		this.threePointFieldGoal = threePointFieldGoal;
		this.threePointFieldGoalAttempts = threePointFieldGoalAttempts;
		this.freeThrow = freeThrow;
		this.freeThrowAttempts = freeThrowAttempts;
		this.offensiveRebound = offensiveRebound;
		this.defensiveRebound = defensiveRebound;
		this.opponentOffensiveRebound = opponentOffensiveRebound;
		this.opponentDefensiveRebound = opponentDefensiveRebound;
		this.backboard = backboard;
		this.assist = assist;
		this.steal = steal;
		this.block = block;
		this.turnOver = turnOver;
		this.foul = foul;
		this.scoring = scoring;
		this.fieldGoalPercentage = fieldGoalPercentage;
		this.threePointShotPercentage = threePointShotPercentage;
		this.freeThrowPercentage = freeThrowPercentage;
		this.winningPercentage = winningPercentage;
		this.possessions = possessions;
		this.offensiveEfficiency = offensiveEfficiency;
		this.defensiveEfficiency = defensiveEfficiency;
		this.offensivebackboardEfficiency = offensivebackboardEfficiency;
		this.defensivebackboardEfficiency = defensivebackboardEfficiency;
		this.stealEfficiency = stealEfficiency;
		this.assitEfficiency = assitEfficiency;
	}
	public int getOpponentFieldGoal() {
		return opponentFieldGoal;
	}
	public void setOpponentFieldGoal(int opponentFieldGoal) {
		this.opponentFieldGoal = opponentFieldGoal;
	}
	public int getOpponentFieldGoalAttempts() {
		return opponentFieldGoalAttempts;
	}
	public void setOpponentFieldGoalAttempts(int opponentFieldGoalAttempts) {
		this.opponentFieldGoalAttempts = opponentFieldGoalAttempts;
	}
	public int getOpponentTurnOver() {
		return opponentTurnOver;
	}
	public void setOpponentTurnOver(int opponentTurnOver) {
		this.opponentTurnOver = opponentTurnOver;
	}
	public int getOpponentFreeThrowAttempts() {
		return opponentFreeThrowAttempts;
	}
	public void setOpponentFreeThrowAttempts(int opponentFreeThrowAttempts) {
		this.opponentFreeThrowAttempts = opponentFreeThrowAttempts;
	}
	public int getOppenentScoring() {
		return oppenentScoring;
	}
	public void setOppenentScoring(int oppenentScoring) {
		this.oppenentScoring = oppenentScoring;
	}
	public String getTeamName() {
		return teamName;
	}
	public void setTeamName(String teamName) {
		this.teamName = teamName;
	}
	public int getMatches() {
		return matches;
	}
	public void setMatches(int matches) {
		this.matches = matches;
	}
	public int getWins() {
		return wins;
	}
	public void setWins(int wins) {
		this.wins = wins;
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
	public int getThreePointFieldGoal() {
		return threePointFieldGoal;
	}
	public void setThreePointFieldGoal(int threePointFieldGoal) {
		this.threePointFieldGoal = threePointFieldGoal;
	}
	public int getThreePointFieldGoalAttempts() {
		return threePointFieldGoalAttempts;
	}
	public void setThreePointFieldGoalAttempts(int threePointFieldGoalAttempts) {
		this.threePointFieldGoalAttempts = threePointFieldGoalAttempts;
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
	public int getOpponentOffensiveRebound() {
		return opponentOffensiveRebound;
	}
	public void setOpponentOffensiveRebound(int opponentOffensiveRebound) {
		this.opponentOffensiveRebound = opponentOffensiveRebound;
	}
	public int getOpponentDefensiveRebound() {
		return opponentDefensiveRebound;
	}
	public void setOpponentDefensiveRebound(int opponentDefensiveRebound) {
		this.opponentDefensiveRebound = opponentDefensiveRebound;
	}
	public int getBackboard() {
		return backboard;
	}
	public void setBackboard(int backboard) {
		this.backboard = backboard;
	}
	public int getAssist() {
		return assist;
	}
	public void setAssist(int assist) {
		this.assist = assist;
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
	public double getFieldGoalPercentage() {
		return fieldGoalPercentage;
	}
	public void setFieldGoalPercentage(double fieldGoalPercentage) {
		this.fieldGoalPercentage = fieldGoalPercentage;
	}
	public double getThreePointShotPercentage() {
		return threePointShotPercentage;
	}
	public void setThreePointShotPercentage(double threePointShotPercentage) {
		this.threePointShotPercentage = threePointShotPercentage;
	}
	public double getFreeThrowPercentage() {
		return freeThrowPercentage;
	}
	public void setFreeThrowPercentage(double freeThrowPercentage) {
		this.freeThrowPercentage = freeThrowPercentage;
	}
	public double getWinningPercentage() {
		return winningPercentage;
	}
	public void setWinningPercentage(double winningPercentage) {
		this.winningPercentage = winningPercentage;
	}
	public double getPossessions() {
		return possessions;
	}
	public void setPossessions(double possessions) {
		this.possessions = possessions;
	}
	public double getOffensiveEfficiency() {
		return offensiveEfficiency;
	}
	public void setOffensiveEfficiency(double offensiveEfficiency) {
		this.offensiveEfficiency = offensiveEfficiency;
	}
	public double getDefensiveEfficiency() {
		return defensiveEfficiency;
	}
	public void setDefensiveEfficiency(double defensiveEfficiency) {
		this.defensiveEfficiency = defensiveEfficiency;
	}
	public double getOffensivebackboardEfficiency() {
		return offensivebackboardEfficiency;
	}
	public void setOffensivebackboardEfficiency(double offensivebackboardEfficiency) {
		this.offensivebackboardEfficiency = offensivebackboardEfficiency;
	}
	public double getDefensivebackboardEfficiency() {
		return defensivebackboardEfficiency;
	}
	public void setDefensivebackboardEfficiency(double defensivebackboardEfficiency) {
		this.defensivebackboardEfficiency = defensivebackboardEfficiency;
	}
	public double getStealEfficiency() {
		return stealEfficiency;
	}
	public void setStealEfficiency(double stealEfficiency) {
		this.stealEfficiency = stealEfficiency;
	}
	public double getAssitEfficiency() {
		return assitEfficiency;
	}
	public void setAssitEfficiency(double assitEfficiency) {
		this.assitEfficiency = assitEfficiency;
	}
	
	
}
