<%--
  Created by IntelliJ IDEA.
  User: josh
  Date: 3/5/14
  Time: 1:16 PM
--%>

<%@ page import="edu.msu.mi.gwurk.Credentials; edu.msu.mi.gwurk.Credentials; edu.msu.mi.gwurk.Workflow" contentType="text/html;charset=UTF-8" %>
<html>
<head>
    <title></title>
</head>

<body>
<g:set var="workflow" value="${flash.workflow as Workflow}"/>
<h1>Launch Workflow: ${workflow.name}</h1>
<p>
   <b>Description:</b>${workflow.description}
</p>
<g:form action="doLaunch">
    <p>

    Type of run? <g:select from="${["sandbox","real"]}" name="type" value="sandbox"/> <br/>
    Iterations?  <g:field name="iterations" type="number" value="1"/><br/>
    Credentials? <g:select name="credentials" from="${edu.msu.mi.gwurk.Credentials.list()}" optionKey="id" optionValue="name"/><br/>

    </p>
    <h2>Global Properties</h2>
    <g:render template="/taskPropertiesForm" model="${[prefix:"global",props:workflow.taskProperties]}"/>
    <g:each in="${workflow.allTasks.values()}" var="task">
        <h2>Task Properties: ${task.name}</h2>
        <g:render template="/taskPropertiesForm" model="${[prefix:task.name,props:task.taskProperties]}"/>
    </g:each>
    <g:submitButton name="Launch" value="Launch"/>
    <g:actionSubmit action="index" value="Cancel"/>
</g:form>

</body>
</html>