package data.initial;

import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class InitialTeaminfo {
//初始化球队信息
	String info="";
	public InitialTeaminfo(Statement statement) {
		System.out.println("初始化球队信息……");
		ReadIn();
		String[] singleinfo=info.split("%");
		for (int i = 0; i < singleinfo.length; i++) {
			String[] temp=singleinfo[i].split(";");
			String sql="INSERT INTO teaminfo values('"
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
					+ temp[6] + "')";
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
		try {
			FileReader fr=new FileReader("data/teams/teams");
			@SuppressWarnings("resource")
			BufferedReader br=new BufferedReader(fr);
			String line="";
			br.readLine();
			for (int i = 0; i < 30; i++) {
				line=br.readLine();
				String[] item=line.split("│");
				line="";
				Pattern p = Pattern.compile("\\s*|\t|\r");  
				for (int j = 0; j < item.length; j++) {
		            Matcher m = p.matcher(item[j]);
					item[j]=m.replaceAll("");
					line=line+item[j]+";";
				}
				line=line.substring(1,line.length()-2);
				info=info+line+"%";
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
}
