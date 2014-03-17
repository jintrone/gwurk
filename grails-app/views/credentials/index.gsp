<%--
  Created by IntelliJ IDEA.
  User: josh
  Date: 3/4/14
  Time: 11:08 PM
--%>

<%@ page import="edu.msu.mi.turkmdr.Credentials" contentType="text/html;charset=UTF-8" %>
<html>
<head>
    <title>Credentials</title>
</head>

<body>
<h1>Credentials</h1>

<g:set var="credentials" value="${Credentials.list()}"/>
<g:if test="${!credentials}">
    <h2>No credentials loaded.</h2>
</g:if>
<g:else>
    <table>
        <thead>

        <tr>
            <td>
                Name
            </td>
            <td>
                AWS ID
            </td>
        </tr>
        </thead>

        <g:each in="${Credentials.list()}" var="c">
            <tr>
                <td>${c.name}</td>
                <td>${c.awsId}</td>
            </tr>
        </g:each>
    </table>
</g:else>
<g:form action="add">
    <table>
        <tr>
            <td>
                Name:<g:textField name="name"/>
            </td>
            <td>
                Aws ID:<g:textField name="awsId"/>
            </td>
            <td>
                Aws Secret:<g:passwordField name="awsSecret"/>
            </td>
        </tr>
    </table>
    <g:submitButton name="Submit" value="Add new credential"/>
</g:form>

</body>
</html>