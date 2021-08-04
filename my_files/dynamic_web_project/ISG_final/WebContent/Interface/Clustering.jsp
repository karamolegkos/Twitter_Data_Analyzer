<%@ page language="java" contentType="text/html; charset=ISO-8859-1"
    pageEncoding="ISO-8859-1"%>
<%@ page import="isg.classes.ClusteringClass" %>
<!DOCTYPE html>
<html>
<head>
<meta charset="ISO-8859-1">
<title>Twitter Data Analyzer - Clustering</title>
<link rel="stylesheet" type="text/css" href="styles/TwitterAnalyzerStyle.css">
</head>
<body>
<%!int testID = 0; %>
<%
	if(request.getParameter("toEnd")!=null){	
		session.setAttribute("phase","end");
		response.sendRedirect("End.jsp"); 
	}
	else if(session.getAttribute("phase") == null){
		response.sendRedirect("Home.jsp");
	}
	else if(!session.getAttribute("phase").equals("clustering")){
		session.removeAttribute("phase");
		response.sendRedirect("Home.jsp");
	}
	else{
		String topic = (String)session.getAttribute("topicName");
		
		%>
		<h1>Clustering phase</h1>
		<h3>You can choose the parameters below to do your K-means clustering.</h3>
		<ul>
			<li>Click the <b>"Do the clustering"</b> button to execute your clustering algorithm.</li>
			<li>Click the <b>"End"</b> button to end the application</li>
		</ul>
		<u>Cluster informations will be produced at the bottom of the page!</u><br><br>
		<div class="form">
			<form>
				<label for="algorithmToUse">Algorithm to use:</label><br>
		  		<select id="algorithmToUse" name="algorithmToUse" required>
		  			  <option value="Euclidean distance">Euclidean distance</option>
					  <option value="Squared Euclidean distance">Squared Euclidean distance</option>
					  <option value="Cosine distance">Cosine distance</option>
					  <option value="Manhattan distance">Manhattan distance</option>
					  <option value="Tanimoto distance">Tanimoto distance</option>
		  		</select><br>
			
				<label for="amountOfClusters">Amount of Clusters (positive integer):</label><br>
		  		<input type="number" id="amountOfClusters" name="amountOfClusters" min="2" required><br>
		  		
		  		<label for="amountOfIterations">Amount of Iterations (positive integer):</label><br>
		  		<input type="number" id="amountOfIterations" name="amountOfIterations" min="2" required><br>
		  		
		  		<label for="deltaConvergence">Delta Convergence value (double - max four decimal digits):</label><br>
		  		<input type="number" step="0.0001" min="0.0000" max="1.0000" id="deltaConvergence" name="deltaConvergence" required><br><br>
			
				<input type="submit" value="Do the clustering" name="DoTheClustering">
			</form>
			<form>
				<input type="submit" value="End" name="toEnd">
			</form>
		</div>
		<%
		
		if(request.getParameter("DoTheClustering")!=null){
			int k = Integer.parseInt(request.getParameter("amountOfClusters"));
			int iterations = Integer.parseInt(request.getParameter("amountOfIterations"));
			String algoPref = request.getParameter("algorithmToUse");
			double delta = Double.parseDouble(request.getParameter("deltaConvergence"));
			
			ClusteringClass.kMeans(topic, ++testID, k, iterations, delta, algoPref);
			
			%><h1>Below are the results for your clustering!</h1>
			Those results have been saved in the HDFS directory below:<br>
			<a target="_blank" href="http://localhost:9870/explorer.html#/<%=topic%>/clustering/clustered<%=testID%>">Clustering number <%=testID%></a><br>
			You can do as many clustering tests you want and the results will be 
			printed here and saved always in the above directory. <br>
			<u>(for every new test, the above link will change
			to the new HDFS directory for your test).</u>
			<h3>Results:</h3>
			<div class="context"><%
			out.println(ClusteringClass.getResults());
			%></div><%
			
		}
		
	}
%>
</body>
</html>