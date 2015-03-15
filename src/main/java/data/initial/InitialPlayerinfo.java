package data.initial;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql.SQLException;
import java.sql.Statement;

public class InitialPlayerinfo {
//初始化球员基本信息
	public static String[] filelist;   //球员姓名
	String info="";
	public InitialPlayerinfo(Statement statement) {
		System.out.println("初始化球员基本信息……");
		ReadIn();
		String[] singleinfo=info.split("%");
		for (int i = 0; i < singleinfo.length; i++) {
			String[] temp=singleinfo[i].split(";");
			for (int j = 0; j < temp.length; j++) {
				if(temp[j].contains("'"))
					temp[j]=temp[j].substring(0, temp[j].indexOf("'"))+"\\"+temp[j].substring(temp[j].indexOf("'"), temp[j].length());
			}
			String sql="INSERT INTO playerinfo (name,number,position,height,weight,birth,age,exp,school) values('"
					+ temp[0]
					+ "','"
					+ temp[1]
					+ "','"
					+ temp[2]
					+ "','"
					+ temp[3]
					+ "','"
					+ temp[4]
					+ "','"
					+ temp[5]
					+ "','"
					+ temp[6]
					+ "','"
					+ temp[7]
					+ "','"
					+ temp[8] + "')";
			try {
				statement.addBatch(sql);
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}  
		}
		try {
			statement.executeBatch();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void ReadIn(){
		File f=new File("data/players/info");
		filelist=f.list();
		for (int i = 0; i <filelist.length; i++) {
			try {
				FileReader fr=new FileReader("data/players/info/"+filelist[i]);
				@SuppressWarnings("resource")
				BufferedReader br=new BufferedReader(fr);
				String line="";
				while((line=br.readLine())!=null){
					if(line.contains("│")){
						String temp=line.substring(line.indexOf("│")+1);
						if(temp.contains("\t"))
							temp=temp.substring(0, temp.indexOf("\t"));
						else
							temp=temp.substring(0, temp.indexOf("║"));
						info=info+temp+";";
					}else{
						continue;
					}
				}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			info=info.substring(0, info.length()-1)+"%";
		}
	}
}
