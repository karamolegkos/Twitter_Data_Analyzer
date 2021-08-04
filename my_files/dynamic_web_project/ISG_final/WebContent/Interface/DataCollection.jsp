<%@ page language="java" contentType="text/html; charset=ISO-8859-1"
    pageEncoding="ISO-8859-1"%>
<%@ page import="isg.classes.CommandLine" %>
<%@ page import="isg.classes.KafkaTwitterProducer" %>
<%@ page import="isg.classes.AvroClass" %>

<!DOCTYPE html>
<html>
<head>
<meta charset="ISO-8859-1">
<title>Twitter Data Analyzer - Data Collection</title>
<link rel="stylesheet" type="text/css" href="styles/TwitterAnalyzerStyle.css">
</head>
<body>
<%
	if(request.getParameter("toAvroPhases")!=null){	
		session.setAttribute("phase","AVRO phases");
		response.sendRedirect("Avro.jsp"); 
	}
	else if(session.getAttribute("phase") == null){
		response.sendRedirect("Home.jsp"); 
	}
	else if(!session.getAttribute("phase").equals("Data Collection Class")){
		session.removeAttribute("phase");
		response.sendRedirect("Home.jsp");
	}
	else{
		String consumerKey = (String)session.getAttribute("consumerKey");
		String consumerSecret = (String)session.getAttribute("consumerSecret");
		String token = (String)session.getAttribute("token");
		String secret = (String)session.getAttribute("secret");
		int amountOfTweets = (int)session.getAttribute("amountOfTweets");
		String topicName = (String)session.getAttribute("topicName");
		String[] keywords = (String[])session.getAttribute("keywords");
		
		if(request.getParameter("gaveFileName")==null){
			CommandLine.runKafka();
			CommandLine.runHDFSYARN();
			CommandLine.waitForSeconds(30);
			CommandLine.addHDFSDir(topicName);
			CommandLine.waitForSeconds(30);
			CommandLine.runFlumeAgent(topicName);
			CommandLine.waitForSeconds(30);
			
			KafkaTwitterProducer.getKafkaTweets(consumerKey,	
					consumerSecret,					
					token,							
					secret,							
					keywords,							
					topicName,					
					amountOfTweets);
			CommandLine.waitForSeconds(10);
		}
		%>
		<h1>Data Collection phase</h1>
		<h3><u>To continue, stop the CMD window that was running the Flume Agent by pressing CTRL+C until it stops (The last CMD that opened)</u>, then you can close this CMD window.</h3>
		Insert the name of the file that Flume created in the HDFS directory below:<br>
		<a target="_blank" href="http://localhost:9870/explorer.html#/<%=topicName%>/mytweets">Tweeter data</a><br>
		<u>(You want something like: FlumeData.1625350796794 - Remember to Refresh the HDFS so you will not get a .tmp file!)</u><br><br>
		<div class="form">
			<form>
				<label for="fileName">Name of the Flume file (with the suffix):</label><br>
				<input type="text" id="fileName" name="fileName" required><br>
				<br>
				Press this button to view the tweets that you collected:<br>
				<input type="submit" value="VIEW TWEETS" name="gaveFileName">
			</form>
		</div>
		
		<h3>When ready, press the button below to save your tweets in AVRO format and go to the next page, so you can deserialize them also.</h3>
		<form>
			<input type="submit" value="FORMAT TWEETS IN AVRO" name="toAvroPhases">
		</form>
		
		<%
		if(request.getParameter("gaveFileName")!=null){
			%><h3>Your Data:</h3><%
		}
		%>
		
		<%
		if(request.getParameter("gaveFileName")!=null){
			
			%><div class="context"><%
			
			String fileName = request.getParameter("fileName");
			session.setAttribute("firstDataFileName",fileName);
			String topic = topicName;
			String realTweets = AvroClass.getHDFSFileContent(topic, "mytweets", fileName);
			out.println(realTweets);
			%></div><%
		}
		%>
		
		<%
	}
%>
</body>
</html>