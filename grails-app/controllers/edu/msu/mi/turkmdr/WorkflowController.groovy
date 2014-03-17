package edu.msu.mi.turkmdr

class WorkflowController {

     def mturkMonitorService

    def index() {}

    def launchWorkflow() {
        if (params.workflow) {
            flash.workflow = Workflow.get(params.workflow)

        } else {
            render(view:"index")
        }

    }

    def doLaunch() {
        Workflow w = flash.workflow

        mturkMonitorService.launch(w,params.type=="real",params.iterations as int,Credentials.get(params.credentials as long), params.props as Map)
        redirect(action:"index",controller:"workflowRun")
    }

    def external() {
        TaskRun run = TaskRun.get(params.task as long)
        [workerId: params.workerId, action: run.taskProperties.action,controller: run.taskProperties.controller, submiturl: run.submitUrl, assignmentId: params.assignmentId]

    }


}
