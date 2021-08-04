<%@ page language="java" contentType="text/html; charset=ISO-8859-1"
    pageEncoding="ISO-8859-1"%>
<%@ page import="isg.classes.CommandLine" %>
<!DOCTYPE html>
<html>
<head>
<meta charset="ISO-8859-1">
<title>Twitter Data Analyzer - Home</title>
<link rel="stylesheet" type="text/css" href="styles/TwitterAnalyzerStyle.css">
</head>
<body>

<%
	if(request.getParameter("initialData")!=null){
		
		int keywordsAmount = Integer.parseInt(request.getParameter("keywordsAmount"));
		String[] keywords = new String[keywordsAmount];
		for(int i=0; i<keywordsAmount; i++){
			// changed to "keyword"+i
			keywords[i] = request.getParameter("keyword"+i).toLowerCase();
		}
		
		session.setAttribute("phase","Data Collection Class");
		session.setAttribute("consumerKey",request.getParameter("consumerKey"));
		session.setAttribute("consumerSecret",request.getParameter("consumerSecret"));
		session.setAttribute("token",request.getParameter("token"));
		session.setAttribute("secret",request.getParameter("secret"));
		session.setAttribute("amountOfTweets",Integer.parseInt(request.getParameter("amountOfTweets")));
		session.setAttribute("topicName",request.getParameter("topicName"));
		session.setAttribute("keywordsAmount",keywordsAmount);
		session.setAttribute("keywords",keywords);
		
		response.sendRedirect("DataCollection.jsp");
	}
%>

	<h1>Twitter Data Analyzer</h1>
	<p>Give your data and preferences below and then click the <b>"Go"</b> button.<br>
	After you press the button, depending on the amount of tweets that you want to collect, <u>it may take a long time to load the next page!</u><br>
	<b>Do not close any CMD windows from the ones that will be opened by this application unless if the application tells you to do it!</b></p>
	<div class="form">
		<form>
		  <label for="consumerKey">Consumer Key:</label><br>
		  <input type="text" id="consumerKey" name="consumerKey" required><br>
		  
		  <label for="consumerSecret">Consumer Secret:</label><br>
		  <input type="text" id="consumerSecret" name="consumerSecret" required><br>
		  
		  <label for="token">Token:</label><br>
		  <input type="text" id="token" name="token" required><br>
		  
		  <label for="secret">Secret:</label><br>
		  <input type="text" id="secret" name="secret" required><br>
		  
		  <label for="amountOfTweets">Amount of Tweets to collect (positive integer):</label><br>
		  <input type="number" id="amountOfTweets" name="amountOfTweets" min="1" required><br>
		  
		  <label for="topicName">Your Topic Name:</label><br>
		  <input type="text" id="topicName" name="topicName" required><br>
		  
		  <div class="keywords">
		  	  <label for="keywordsAmount">Amount of Keywords to search for (positive integer):</label><br>
			  <input type="number" id="keywordsAmount" name="keywordsAmount" min="1" value="1" required><br>
			  
			  <button class="button" type="button" onclick = "openKeywordsFields()">Get keyword fields</button><br><br>
			  
			  <div id="keywordFields">
			  	<label for="keyword0">Keyword 1:</label><br>
			  	<input type="text" id="keyword0" name="keyword0" required>
			  </div>
		  </div>
		  
		  <input class="button" type="submit" value="Go" name="initialData">
		</form>
	</div>

	<script>
		function openKeywordsFields(){
			let element = document.getElementById("keywordFields");
			element.innerHTML = "";
			for(let i = 0; i< document.getElementById("keywordsAmount").value; i++ ){
				let label = document.createElement("label");
				label.for = "keyword"+i;
				label.innerHTML  = "Keyword "+(i+1)+":";
				
				let input = document.createElement("input");
				input.type = "text";
				input.id = "keyword"+i;
				input.name = "keyword"+i;
				input.required = true;
				
				element.appendChild(label);
				element.appendChild(input);
				element.innerHTML += "<br>";
			}
		}
	</script>
</body>
</html>