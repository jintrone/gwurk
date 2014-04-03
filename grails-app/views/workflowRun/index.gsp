<%--
  Created by IntelliJ IDEA.
  User: josh
  Date: 3/5/14
  Time: 9:13 PM
--%>

<%@ page import="edu.msu.mi.gwurk.WorkflowRun" contentType="text/html;charset=UTF-8" %>
<html>
<head>
    <title></title>
</head>

<body>
<g:set var="workflowruns" value="${WorkflowRun.list()}"/>
<g:if test="${!workflowruns}">
    <h2>No runs found.</h2>
</g:if>
<g:else>
    <table>
        <thead>

        <tr>
            <td>
                Workflow
            </td>
            <td>
                Status
            </td>
            <td>
                Active Task
            </td>
        </tr>
        </thead>

        <g:each in="${workflowruns}" var="w">
            <tr>
                <td>${w.workflow.name}</td>
                <td>${w.currentStatus}</td>
                <td>${w.currentTasks}</td>
            </tr>
        </g:each>
    </table>
</g:else>



</body>
</html>