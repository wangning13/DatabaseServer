package data.update;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DecimalFormat;
import java.util.TimerTask;

import data.initial.InitialDatabase;
import rmi.Server;

public class UpdateDatabase extends TimerTask{

	Connection conn=null;
	public void run(){
		File f=new File("data/matches");
		String[] matches=f.list();
		for (int i = 0; i < matches.length; i++) {
			String[] temp=matches[i].split("_");
			if(!Server.season.contains(temp[0])){
				createTable(temp[0]);
				Server.season.add(temp[0]);
			}
		}
		if(matches.length!=Server.matches.length){
			updateData(matches,Server.matches);
			Server.matches=matches;
		}
	}
	
	public void updateData(String[] newData,String[] oldData){
		try {
			Class.forName(InitialDatabase.driver);
			conn = DriverManager.getConnection(InitialDatabase.url);
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} 
		if(newData.length>oldData.length){
			for (int i = 0; i < newData.length; i++) {
				boolean notExist=true;
				for (int j = 0; j < oldData.length; j++) {
					if(newData[i].equals(oldData[j])){
						notExist=false;
						break;
					}
				}
				if(notExist){
					//insert into playerdata
					String[] temp=newData[i].split("_");    //12-13
					String[] year=temp[0].split("-");
					String date=temp[1];
					String[] team=temp[2].split("-");
					if(date.startsWith("10-")||date.startsWith("11-")||date.startsWith("12-"))
							date=year[0]+"-"+date;
					else
						date=year[1]+"-"+date;                  //date  13-12-02
					try {
						FileReader fr=new FileReader("data/matches/"+newData[i]);
						BufferedReader br=new BufferedReader(fr);
						String line="";
						int count=0;
						String info="";
						while((line=br.readLine())!=null){
							if(!line.contains(";")){
								count++;
								continue;
							}
							if(line.charAt(0)>=48&&line.charAt(0)<=57){
								continue;
							}else{
								if(count==1)
									info=info+date+";"+team[0]+";"+line.substring(0,line.length()-1)+"%";
								else
									info=info+date+";"+team[1]+";"+line.substring(0,line.length()-1)+"%";
							}
						}
						PreparedStatement ps=conn.prepareStatement("INSERT INTO playerdata  values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
						String[] singleinfo=info.split("%");
						for (int j = 0; j < singleinfo.length; j++) {
							temp=singleinfo[j].split(";");
							if(temp[19].charAt(0)<48||temp[19].charAt(0)>57)
								temp[19]="0";
							double time=0;
							DecimalFormat df=new DecimalFormat("#.0");  
							if(temp[4].contains(":")){
								String[] temp1=temp[4].split(":");
								time=Double.parseDouble(temp1[0])+Double.parseDouble(df.format(Double.parseDouble(temp1[1])/60));
							}else if(temp[4].charAt(0)>=48&&temp[4].charAt(0)<=57){
								time=Double.parseDouble(df.format(Double.parseDouble(temp[4])/60));
							}
							ps.setString(1, temp[0]); 
							ps.setString(2, temp[1]); 
							ps.setString(3, temp[2]); 
							ps.setString(4, temp[3]); 
							ps.setDouble(5, time);
							ps.setInt(6, Integer.parseInt(temp[5]));
							ps.setInt(7, Integer.parseInt(temp[6]));
							ps.setInt(8, Integer.parseInt(temp[7]));
							ps.setInt(9, Integer.parseInt(temp[8]));
							ps.setInt(10, Integer.parseInt(temp[9]));
							ps.setInt(11, Integer.parseInt(temp[10]));
							ps.setInt(12, Integer.parseInt(temp[11]));
							ps.setInt(13, Integer.parseInt(temp[12]));
							ps.setInt(14, Integer.parseInt(temp[13]));
							ps.setInt(15, Integer.parseInt(temp[14]));
							ps.setInt(16, Integer.parseInt(temp[15]));
							ps.setInt(17, Integer.parseInt(temp[16]));
							ps.setInt(18, Integer.parseInt(temp[17]));
							ps.setInt(19, Integer.parseInt(temp[18]));
							ps.setInt(20, Integer.parseInt(temp[19]));
							ps.addBatch();
						}
						ps.executeBatch();
						//insert into matches
						updataMatches(newData[i]);
						
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
		}else{
			
		}
	}
	
	public void createTable(String season){
		try {
			Class.forName(InitialDatabase.driver);
			conn = DriverManager.getConnection(InitialDatabase.url);
			Statement statement=conn.createStatement();
			String sql="CREATE TABLE IF NOT EXISTS `playersum"+season+"` (`playerName`	TEXT,`team`	TEXT,`appearance`	INTEGER,	`firstPlay`	INTEGER,`backboard`	INTEGER,	`assist`	INTEGER,	`minutes`	REAL,`fieldGoal`	INTEGER,`fieldGoalAttempts`	INTEGER,`threePointFieldGoal` INTEGER,`threePointFieldGoalAttempts` INTEGER,`freeThrow`	INTEGER,`freeThrowAttempts` INTEGER, `offensiveRebound` INTEGER, `defensiveRebound`	INTEGER,	`steal` INTEGER, `block`	INTEGER,	`turnOver` INTEGER, `foul` INTEGER, `scoring` INTEGER, `previousAverageScoring` INTEGER, `nearlyFiveAverageScoring` INTEGER,	`doubleDouble` INTEGER, PRIMARY KEY(playerName,team))";
			statement.addBatch(sql);
			sql="CREATE TABLE IF NOT EXISTS `teamsum"+season+"` (`opponentFieldGoal`	INTEGER,`opponentFieldGoalAttempts` INTEGER,`opponentTurnOver` INTEGER,`opponentFreeThrowAttempts`	INTEGER,	`oppenentScoring`	INTEGER,	`teamName`	TEXT,`matches` INTEGER,`wins`	INTEGER,`fieldGoal`	INTEGER,	`fieldGoalAttempts` INTEGER,`threePointFieldGoal`	INTEGER,	`threePointFieldGoalAttempts`	INTEGER,	`freeThrow`	INTEGER,	`freeThrowAttempts`	INTEGER,	`offensiveRebound`	INTEGER,	`defensiveRebound`	INTEGER,	`opponentOffensiveRebound`	INTEGER,	`opponentDefensiveRebound` INTEGER,`backboard`	INTEGER,	`assist`	INTEGER,	`steal`	INTEGER,	`block`	INTEGER,	`turnOver` INTEGER,`foul` INTEGER,`scoring`	INTEGER,	`minutes`	REAL,`opponentBackBoard` INTEGER,`opponentThreePointFieldGoalAttempts`	INTEGER);";
			statement.addBatch(sql);
			statement.executeBatch();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
	}
	
	public void updatePlayersum(String season){
		try {
			Class.forName(InitialDatabase.driver);
			conn = DriverManager.getConnection(InitialDatabase.url);
			PreparedStatement ps=conn.prepareStatement("INSERT INTO `playersum"+season+"`  values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
	}
	
	public void updateTeamsum(String season){
		try {
			Class.forName(InitialDatabase.driver);
			conn = DriverManager.getConnection(InitialDatabase.url);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
	}
	
	public void updataMatches(String fileName){
		try {
			Class.forName(InitialDatabase.driver);
			conn = DriverManager.getConnection(InitialDatabase.url);
			String info="";
			FileReader fr=new FileReader("data/matches/"+fileName);
			BufferedReader br=new BufferedReader(fr);
			String line=br.readLine();
			String[] temp=line.split(";");
			String guest="";
			String[] item=fileName.split("_");
			String[] year=item[0].split("-");
			if(temp[0].startsWith("10-")||temp[0].startsWith("11-")||temp[0].startsWith("12-")){
				info=year[0]+"-"+temp[0]+";h;";
				guest=year[0]+"-"+temp[0]+";g;";
		    }else{
				info=year[1]+"-"+temp[0]+";h;";
				guest=year[1]+"-"+temp[0]+";g;";
		    }
			String[] temp1=temp[1].split("-");
			info=info+temp1[0]+";"+temp1[1]+";";
			guest=guest+temp1[1]+";"+temp1[0]+";";
			temp1=temp[2].split("-");
			if(Integer.parseInt(temp1[0])>Integer.parseInt(temp1[1])){
				info=info+"w;"+temp1[0]+";";
				guest=guest+"l;"+temp1[1]+";";
			}else{
				info=info+"l;"+temp1[0]+";";
				guest=guest+"w;"+temp1[1]+";";
			}
			line=br.readLine();
			temp=line.split(";");
			for (int j = 0; j < 4; j++) {
				temp1=temp[j].split("-");
				info=info+temp1[0]+";";
				guest=guest+temp1[1]+";";
			}
			info=info.substring(0, info.length()-1);
			PreparedStatement ps=conn.prepareStatement("INSERT INTO matches  values(?,?,?,?,?,?,?,?,?,?)");
			temp=info.split(";");
			ps.setString(1, temp[0]);
			ps.setString(2, temp[1]);
			ps.setString(3, temp[2]);
			ps.setString(4, temp[3]);
			ps.setString(5, temp[4]);
			ps.setInt(6, Integer.parseInt(temp[5]));
			ps.setInt(7, Integer.parseInt(temp[6]));
			ps.setInt(8, Integer.parseInt(temp[7]));
			ps.setInt(9, Integer.parseInt(temp[8]));
			ps.setInt(10, Integer.parseInt(temp[9]));
			ps.addBatch();
			temp=guest.split(";");
			ps.setString(1, temp[0]);
			ps.setString(2, temp[1]);
			ps.setString(3, temp[2]);
			ps.setString(4, temp[3]);
			ps.setString(5, temp[4]);
			ps.setInt(6, Integer.parseInt(temp[5]));
			ps.setInt(7, Integer.parseInt(temp[6]));
			ps.setInt(8, Integer.parseInt(temp[7]));
			ps.setInt(9, Integer.parseInt(temp[8]));
			ps.setInt(10, Integer.parseInt(temp[9]));
			ps.addBatch();
			ps.executeBatch();
			conn.commit(); 
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
}
