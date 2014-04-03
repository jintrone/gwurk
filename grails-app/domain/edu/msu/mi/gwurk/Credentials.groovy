package edu.msu.mi.gwurk

class Credentials {

    static constraints = {
    }

    static hasMany = [workflowRuns:WorkflowRun]

    String awsId
    String awsSecret
    String name
}
