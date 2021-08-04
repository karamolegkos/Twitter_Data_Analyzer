<%@ page language="java" contentType="text/html; charset=ISO-8859-1"
    pageEncoding="ISO-8859-1"%>
<%@ page import="isg.classes.AvroClass" %>
<!DOCTYPE html>
<html>
<head>
<meta charset="ISO-8859-1">
<title>Twitter Data Analyzer - AVRO phases</title>
<link rel="stylesheet" type="text/css" href="styles/TwitterAnalyzerStyle.css">
</head>
<body>
<%
	if(request.getParameter("toMapReduce")!=null){	
		session.setAttribute("phase","Map Reduce Job");
		response.sendRedirect("MapReduce.jsp"); 
	}
	else if(session.getAttribute("phase") == null){
		response.sendRedirect("Home.jsp");
	}
	else if(!session.getAttribute("phase").equals("AVRO phases")){
		session.removeAttribute("phase");
		response.sendRedirect("Home.jsp");
	}
	else{
		String topic = (String)session.getAttribute("topicName");
		String fileName = (String)session.getAttribute("firstDataFileName");
		%>
		
			<%
				if(request.getParameter("DoTheDeserialize")==null){
					AvroClass.avroSerialize(topic, fileName);
				}
			%>
			<h1>AVRO phase</h1>
			<h3>Your data is now saved in AVRO format.</h3>
			You can see your data (before and after the deserilization) in the HDFS directory below:<br>
			<a target="_blank" href="http://localhost:9870/explorer.html#/<%=topic%>/avro">Serialized and Deserialized AVRO directory</a><br>
			<u>Only the user's name, the tweet's text and the timestamp of each tweet have been saved!</u><br><br>
			<div class="form">
				<form>
					Press the <b>"Deserialize Data"</b> button to Deserialize your tweets from AVRO format and view them below.
					<input type="submit" value="Deserialize Data" name="DoTheDeserialize">
				</form>
				
				<form>
					When ready, press the button below to do a Map Reduce Job on your tweets, counting the amount of times your searching keywords exist inside of them.
					<input type="submit" value="Map Reduce Job" name="toMapReduce">
				</form>
			</div>
			<%
			if(request.getParameter("DoTheDeserialize")!=null){
				%><h3>Below is the saved information for your tweets deserialized from AVRO format:</h3><%
			}
			%>
			
			<%
				if(request.getParameter("DoTheDeserialize")!=null){
					
					%><div class="context"><%
							
					AvroClass.avroDeserialize(topic);
					String realTweets = AvroClass.getHDFSFileContent(topic, "avro", "desertweets.txt");
					out.println(realTweets);
					
					%></div><%
				}
			%>
		<%
	}

%>
</body>
</html>