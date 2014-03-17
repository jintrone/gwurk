package edu.msu.mi.turkmdr

import grails.transaction.Transactional
import groovy.util.logging.Log4j


@Transactional
@Log4j
class MturkTaskService {

    def assignmentFx = [:]
    def hitFx = [:]
    def taskFx = [:]
    def workflowFx = [:]



    def onAssignment(TaskRun taskRun, AssignmentView assignmentView) {
        log.info("On Assignment: ${taskRun.task.name}")
        assignmentFx[taskRun.task.name](new GwurkEvent(taskRun,assignmentView))
    }

    def onHit(TaskRun taskRun, HitView hitView) {
        log.info("On Hit: ${taskRun.task.name}")
        hitFx[taskRun.task.name](new GwurkEvent(taskRun,hitView))
    }

    def onTask(TaskRun taskRun) {
        log.info("On Task: ${taskRun.task.name}")
        taskFx[taskRun.task.name](new GwurkEvent(taskRun))
    }

    def onWorkflow(WorkflowRun workflowRun) {
        log.info("On Workflowt: ${workflowRun.workflow.name}")
        workflowFx[workflowRun.workflow.name](new GwurkEvent(workflowRun))
    }


    def installTask(Task t, Closure c) {
        if (c) {
            taskFx+=[ (t.name) : {evt -> c(GwurkEvent.Type.TASK_COMPLETE,evt)}]
            assignmentFx+=[ (t.name) :{evt -> c(GwurkEvent.Type.ASSIGNMENT_COMPLETE,evt)}]
            hitFx+=[ (t.name) :{evt -> c(GwurkEvent.Type.HIT_COMPLETE,evt)}]

        }
    }

    def installWorkflow(Workflow w, Closure closure) {
       if (closure) {
           workflowFx+=[ (w.name) :{evt -> closure(GwurkEvent.Type.WORKFLOW_COMPLETE,evt)}]
       }
    }
}
