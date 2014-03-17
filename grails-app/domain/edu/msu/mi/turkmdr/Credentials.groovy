package edu.msu.mi.turkmdr

class Credentials {

    static constraints = {
    }

    static hasMany = [workflowRuns:WorkflowRun]

    String awsId
    String awsSecret
    String name
}
