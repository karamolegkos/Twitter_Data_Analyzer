<%@ page language="java" contentType="text/html; charset=ISO-8859-1"
    pageEncoding="ISO-8859-1"%>
<%@ page import="isg.classes.Preprocess" %>
<!DOCTYPE html>
<html>
<head>
<meta charset="ISO-8859-1">
<title>Twitter Data Analyzer - Preprocessing</title>
<link rel="stylesheet" type="text/css" href="styles/TwitterAnalyzerStyle.css">
<style>
table, th, td {
  border: 1px solid black;
}
</style>
</head>
<body>
<%
	if(request.getParameter("toClustering")!=null){	
		session.setAttribute("phase","clustering");
		response.sendRedirect("Clustering.jsp"); 
	}
	else if(session.getAttribute("phase") == null){
		response.sendRedirect("Home.jsp");
	}
	else if(!session.getAttribute("phase").equals("preprocessing")){
		session.removeAttribute("phase");
		response.sendRedirect("Home.jsp");
	}
	else{
		String topic = (String)session.getAttribute("topicName");
		String[] keywords = (String[])session.getAttribute("keywords");
		%>
		<h1>Preprocessing phase</h1>
		<h3>Your data have been preprocessed and you can see the preprocessing steps here.</h3>
		The preprocessed data are saved inside your HDFS also, in the directory below:<br>
		<a target="_blank" href="http://localhost:9870/explorer.html#/<%=topic%>/preprocessed">Preprocessed data directory</a><br><br>
		<div class="form">
			<form>
				When ready, press the button below to start the clustering phase of your data.
				<input type="submit" value="Cluster your data" name="toClustering">
			</form>
		</div>
		<br>
		<h3>-> First Step: Holding only the tweets alone:</h3>
		<div class="context">
		<%
		String[] tweets = Preprocess.holdOnlyTweets(topic);
		%><ol><%
		for(String tweet : tweets){
			%><li><%
			out.print(tweet);
			%></li><%
		}
		%></ol>
		</div>
		<h3>-> Second Step: Counted the amount of times each searching term is inside of each tweet:</h3>
		<div class="context">
		<%
		Integer[][] preprocessed = Preprocess.arrayTweetsBasedOnTerms(tweets, keywords);
		Preprocess.savePreDataToHDFS(topic, preprocessed, tweets.length, keywords.length);
		%>
		<table style="border: 1px solid black">
		  <thead>
		    <tr>
		    	<th>Tweet\term</th>
		    	<%
		    	for(String keyword : keywords){
		    		%><th><%=keyword%></th><%
		    	}
		    	%>
		    </tr>
		   </thead>
		   <tbody>
		   		<%
		    	for(int i=0; i<tweets.length; i++){
		    		int a = i + 1;
		    		%>
		    		<tr>
		    			<td><%=a%></td>
		    			<%
				    	for(int j=0; j<keywords.length; j++){
				    		%><td><%=preprocessed[i][j]%></td><%
				    	}
				    	%>
		    		</tr>
		    		<%
		    	}
		    	%>
		  </tbody>
		</table>
		</div>
		<%
		
	}
%>
</body>
</html>