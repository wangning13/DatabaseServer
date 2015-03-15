package data.initial;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class InitialMatches {
//初始化比赛数据
	String info="";
	public InitialMatches(Connection conn) {
		System.out.println("初始化比赛数据……");
		ReadIn();
		try {
			PreparedStatement ps=conn.prepareStatement("INSERT INTO matches  values(?,?,?,?,?,?,?,?,?,?)");
			String[] singleinfo=info.split("%");
			for (int i = 0; i < singleinfo.length; i++) {
				String[] singleline=singleinfo[i].split(":");
				for (int j = 0; j < singleline.length; j++) {
					String[] temp=singleline[j].split(";");
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
					if (i % 200 == 0) {  
	                    ps.executeBatch();  
	                    conn.commit();  
	                } 
				}
			}
			ps.executeBatch();
			conn.commit(); 
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void ReadIn(){
		File f=new File("data/matches");
		String[] filelist=f.list();
		for (int i = 0; i < filelist.length; i++) {
			try {
				FileReader fr=new FileReader("data/matches/"+filelist[i]);
				@SuppressWarnings("resource")
				BufferedReader br=new BufferedReader(fr);
				String line=br.readLine();
				String[] temp=line.split(";");
				info=info+temp[0]+";h;";
				String guest=":"+temp[0]+";g;";
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
				info=info.substring(0, info.length()-1)+guest;
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			info=info.substring(0, info.length()-1)+"%";
		}
	}
}
