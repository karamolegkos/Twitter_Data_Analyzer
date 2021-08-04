<%@ page language="java" contentType="text/html; charset=ISO-8859-1"
    pageEncoding="ISO-8859-1"%>
<!DOCTYPE html>
<html>
<head>
<meta charset="ISO-8859-1">
<title>Twitter Data Analyzer - End</title>
<link rel="stylesheet" type="text/css" href="styles/TwitterAnalyzerStyle.css">
</head>
<body>
<%
	if(session.getAttribute("phase") == null){
		response.sendRedirect("Home.jsp");
	}
	else if(!session.getAttribute("phase").equals("end")){
		session.removeAttribute("phase");
		response.sendRedirect("Home.jsp");
	}
	else{
		String topic = (String)session.getAttribute("topicName");
		
		%><h1>Thank you!</h1>
		All of your data during the use of this application have been saved here: 
		<a target="_blank" href="http://localhost:9870/explorer.html#/<%=topic%>"><%=topic%> topic HDFS directory</a>
		<br>
		You can close all of your CMD windows opened by this application.<br>
		<u>It is good to close these windows by pressing CTRL+C on them until they stop and then close them!</u><br>
		To run again the application, you must restart the server after closing all the CMD windows and start again from the Home.jsp endpoint.
		
		<%
	}

%>
</body>
</html>