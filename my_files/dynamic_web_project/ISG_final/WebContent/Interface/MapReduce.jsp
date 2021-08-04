<%@ page language="java" contentType="text/html; charset=ISO-8859-1"
    pageEncoding="ISO-8859-1"%>
<%@ page import="isg.classes.MRRunner" %>
<!DOCTYPE html>
<html>
<head>
<meta charset="ISO-8859-1">
<title>Twitter Data Analyzer - Map Reduce</title>
<link rel="stylesheet" type="text/css" href="styles/TwitterAnalyzerStyle.css">
</head>
<body>
<%
	if(request.getParameter("toPreProcessing")!=null){	
		session.setAttribute("phase","preprocessing");
		response.sendRedirect("Preprocessing.jsp"); 
	}
	else if(session.getAttribute("phase") == null){
		response.sendRedirect("Home.jsp");
	}
	else if(!session.getAttribute("phase").equals("Map Reduce Job")){
		session.removeAttribute("phase");
		response.sendRedirect("Home.jsp");
	}
	else{
		String topic = (String)session.getAttribute("topicName");
		String[] keywords = (String[])session.getAttribute("keywords");
		MRRunner mr = new MRRunner(topic, keywords);
		MRRunner.doMapReduceJob();
		%>
		<h1>Map Reduce Job phase</h1>
		<h3>Below is the results for the Map Reduce Job on your tweets.</h3>
		You can also find the results inside your HDFS in the directory below:<br>
		<a target="_blank" href="http://localhost:9870/explorer.html#/<%=topic%>/mapreducejob">Map Reduce Results</a><br>
		<u>The results will be in the "part-00000" file!</u><br><br>
		<div class="form">
			<form>
				When ready, press the button below to start the preprocessing phase of your data.<br>
				<input type="submit" value="To the preprocess phase" name="toPreProcessing">
			</form>
		</div>
		<h3>Amounts of found searching terms inside the tweets:</h3>
		<div class="context">
		<%
		out.println(MRRunner.getHDFSFileContent(topic));
		%>
		</div>
		<%
		
		
	}
%>
</body>
</html>