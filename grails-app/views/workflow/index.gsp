<%--
  Created by IntelliJ IDEA.
  User: josh
  Date: 3/5/14
  Time: 12:44 PM
--%>

<%@ page import="edu.msu.mi.gwurk.Workflow" contentType="text/html;charset=UTF-8" %>
<html>
<head>
    <title></title>
</head>

<body>
<g:set var="workflows" value="${Workflow.list()}"/>
<g:if test="${!workflows}">
    <h2>No workflows defined.</h2>
</g:if>
<g:else>
    <g:form action="launchWorkflow">
        <table>
            <thead>

            <tr>
                <td>Select</td>
                <td>
                    Name
                </td>
                <td>
                    Description
                </td>
            </tr>
            </thead>

            <g:each in="${workflows}" var="w">
                <tr>
                    <td><g:radio name="workflow" value="${w.id}"/></td>
                    <td>${w.name}</td>
                    <td>${w.description}</td>
                </tr>
            </g:each>
        </table>


        <g:submitButton name="Launch" value="Launch selected"/>
    </g:form>

</g:else>

</body>
</html>