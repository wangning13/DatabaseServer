package data.initial;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.DecimalFormat;

public class InitialPlayerdata {
//初始化球员技术统计
	String info="";
	public InitialPlayerdata(Connection conn) {
		try {
			System.out.println("初始化球员技术统计……");
			PreparedStatement ps=conn.prepareStatement("INSERT INTO playerdata  values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
			ReadIn();
			String[] singleinfo=info.split("%");
			for (int i = 0; i < singleinfo.length; i++) {
				String[] temp=singleinfo[i].split(";");
				if(temp[2].contains("'"))
					temp[2]=temp[2].replaceAll("'", "''");
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
				if (i % 500 == 0) {  
                    ps.executeBatch();  
                    conn.commit();  
                } 
			}
			ps.executeBatch();
			conn.commit(); 
		} catch (NumberFormatException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void ReadIn(){
		File f=new File("data/matches");
		String[] filelist=f.list();
		for (int i = 0; i < filelist.length; i++) {
			String[] temp=filelist[i].split("_");
			if(!temp[0].equals(InitialDatabase.initial_season))
				continue;
			String[] year=temp[0].split("-");
			String date=temp[1];
			String[] team=temp[2].split("-");
			if(date.startsWith("10-")||date.startsWith("11-")||date.startsWith("12-"))
					date=year[0]+"-"+date;
			else
				date=year[1]+"-"+date;
			try {
				FileReader fr=new FileReader("data/matches/"+filelist[i]);
				@SuppressWarnings("resource")
				BufferedReader br=new BufferedReader(fr);
				String line="";
				int count=0;
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
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}