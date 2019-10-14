package com.amazonaws.simples.Server;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.SendMessageRequest;

public class BusinessLogicLayer extends Thread implements SQS{
	String url = "jdbc:sqlserver://database-2.csxrqxrnvv4s.us-east-1.rds.amazonaws.com:1433;databaseName = sapro";
	String user = "admin";
	String password = "12345678";
//	String url = "jdbc:sqlserver://localhost:1433;databaseName = SAProject";
//	String user = "sa";
//	String password = "lkx991127";
	Connection ct = null;
	String message = null;
	String beginYear = null;
	String beginMonth = null;
	String endYear = null;
	String endMonth = null;
	String returnMessage;
	String sql = "";
	Statement statement = null;
	ResultSet rs = null;
	
	public void connectDataBase()
	{
		try {
			Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
			System.out.println("加载驱动成功");
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
			System.out.println("加载驱动失败");
		}
		try {
			ct = DriverManager.getConnection(url,user,password);
			statement = ct.createStatement();
			System.out.println("连接成功");
			System.out.println("业务层：开始查询...");
			if(message.equals("成交量(股)"))
			{
				sql = "select [成交量(股)] " + 
						"from 上证某公司股市数据 " + 
						"where 日期 like '";
//				sql = "select [成交量(股)] " + "from tabletest where 日期 like '";
			}
			else if(message.equals("成交金额(元)"))
			{
				sql = "select [成交金额(元)] " + 
						"from 上证某公司股市数据 " + 
						"where 日期 like '";
//				sql = "select [成交金额(元)] " + "from tabletest where 日期 like '";
			}
			int by = Integer.parseInt(beginYear);
			int bm = Integer.parseInt(beginMonth);
			int ey = Integer.parseInt(endYear);
			int em = Integer.parseInt(endMonth);
			int curMonth = bm;
			for(int count = 0;count<=ey-by;count++)
			{
				for(int i = 0;i<=12-curMonth;i++)
				{
					String tempSql = sql;
					if (count==ey-by&&(curMonth+i)>em) {
						break;
					}
					String time = "";
					if(curMonth+i<10)
					{
						time = String.valueOf(by+count) + "%0" + String.valueOf(curMonth+i) + "%'";
//						time = "0" + String.valueOf(curMonth+i) + "%" + String.valueOf(by+count) + "%'";
						System.out.println("time:"+time);
						tempSql += time;
						
					}else {
						time = String.valueOf(by+count) + "%" + String.valueOf(curMonth+i) + "%'";
//						time = String.valueOf(curMonth+i) + "%" + String.valueOf(by+count) + "%'";
						System.out.println("time:"+time);
						tempSql += time;
					}
					System.out.println(tempSql);
					rs = statement.executeQuery(tempSql);
					double result = 0.0;
					while(rs.next())
					{
						String str = rs.getString(message);
						if(str.equals("N/A"))
							str = "0";
						result += Double.parseDouble(str);
					}
					System.out.println("result:"+result);
					if(returnMessage == null)
					{
						returnMessage = String.valueOf(result);
					}
					else {
						returnMessage += String.valueOf(result);
					}
					returnMessage += ";";
				}
				curMonth = 1;
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	public void returnMessage()
	{
		System.out.println("业务层：开始向客户端返回查询消息...");
		SendMessageRequest send_msg_request = new SendMessageRequest()
	            .withQueueUrl(URL1)
	            .withMessageBody(returnMessage)
	            .withDelaySeconds(0);
	    sqs.sendMessage(send_msg_request);
	   System.out.println("业务层：返回成功...请在客户端查看结果...");
	}
	
	public void run()
	{
		while(true)
		{
			Thread.currentThread();
			try {
				Thread.sleep(10000);
			} catch (InterruptedException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			System.out.println("业务层：业务逻辑层从队列中获取查询信息...");
			List<Message> messages = sqs.receiveMessage(URL).getMessages();
			if(messages.size() > 0)
			{
				for(Message m : messages)
				{
					message = m.getBody();
					sqs.deleteMessage(URL, m.getReceiptHandle());
					String[] str = message.split(";");
					beginYear = str[0];
					beginMonth = str[1];
					endYear = str[2];
					endMonth = str[3];
					message = str[4];
					sql += message;
					sql += "] from tabletest where 日期 like '";
				}
				System.out.println("业务层：已查询到客户端提交的查询信息");
				System.out.println("客户端想查询从" + beginYear + "/" + beginMonth + "到" + endYear + "/" + endMonth + "的" + message);
				connectDataBase();
				returnMessage();
			}
			else {
				System.out.println("业务层持续查询中...");
			}
		}
	}
}
