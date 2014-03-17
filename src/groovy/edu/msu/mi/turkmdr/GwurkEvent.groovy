package edu.msu.mi.turkmdr

/**
 * Created by josh on 3/6/14.
 */
public class GwurkEvent {

    WorkflowRun workflowRun
    TaskRun taskRun
    HitView hitView
    AssignmentView assignmentView

    public static enum Type {
        ASSIGNMENT_COMPLETE, HIT_COMPLETE, TASK_COMPLETE, WORKFLOW_COMPLETE
    }

    GwurkEvent(WorkflowRun workflowRun) {
        this.workflowRun = workflowRun
    }

    GwurkEvent(TaskRun taskRun, HitView hitView) {
        this.taskRun = taskRun
        this.hitView = hitView
    }

    GwurkEvent(TaskRun taskRun, AssignmentView assignmentView) {
        this.taskRun = taskRun
        this.assignmentView = assignmentView
    }

    GwurkEvent(TaskRun taskRun) {
        this.taskRun = taskRun
    }
}
