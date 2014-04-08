package edu.msu.mi.gwurk

import com.amazonaws.mturk.service.axis.RequesterService
import groovy.util.logging.Log4j

@Log4j
class TaskRun implements BeatListener{

    static constraints = {
        activeHit nullable: true
        allHits nullable: true
        previousTaskRuns nullable: true
        activeWorkflowRun nullable: true

    }

    //Note workflowRun and activeWorkflowRun will never point to two different workflows; activeWorkflowRun is only included
    //to speed up access to active taskruns in a running workflow
    static belongsTo = [workflowRun:WorkflowRun,activeWorkflowRun:WorkflowRun]
    static hasMany = [allHits:HitView,previousTaskRuns:TaskRun]
    static mappedBy = [allHits:"taskRun"]


    Task task
    Status taskStatus
    HitView activeHit
    TaskProperties taskProperties
    Map<String,String> userProperties = [:]


    public TaskRun(Task task, TaskProperties props) {
        this.task = task;
        this.taskProperties = props
        this.taskStatus = Status.WAITING
        log.debug "Newly created: ${System.identityHashCode(this)} id: $id"
    }

    def mturkAwsFacadeService
    def mturkTaskService

    /**
     * Checks if there are enough assignments to complete this task
     * @return
     */
    boolean hasAllAssignments() {
        log.debug("Searching for all assignments")
        log.debug("Looking for  ${taskProperties.maxAssignments} assignments")
        log.debug("Approval required by task? "+ taskProperties.requireApproval)
        def assignments = allHits*.assignments.flatten().findAll {
            log.debug("Examining $it; has status ${it.assignmentStatus}")

            taskProperties.requireApproval?it.assignmentStatus == AssignmentView.Status.APPROVED:true
        }
        log.debug("Found completed: ${assignments}")


        assignments.size()>= taskProperties.maxAssignments
    }

    void setUserProperty(String key, String val) {
       userProperties[key] = val
        save()
    }

    String getUserProperty(String key) {
        userProperties[key]
    }

    /**
     * Checks if there are pending assignments
     * @return
     */
    boolean hasPendingAssignments() {
        allHits*.assignments.findAll {
            it.assignmentStatus == AssignmentView.Status.SUBMITTED
        }.size() > 0
    }

    def afterLoad() {
        log.debug "After load: This identity: ${System.identityHashCode(this)} id: $id"
    }

    def afterInsert() {
        log.debug "After insert: This identity: ${System.identityHashCode(this)} id: $id"
    }


    @Override
    def beat(def beater, long timestamp) {
        RequesterService service = (beater as WorkflowRun).requesterService
        if (taskStatus == Status.WAITING) {
            task.mturkTaskService.onTaskStarting(this)
            taskStatus = Status.RUNNING
            log.debug "BEAT: This identity: ${System.identityHashCode(this)} id: $id"
            task.start(service,this)
        } else if (taskStatus in Status.RUN_STATES) {
            log.debug("Updating on status $taskStatus in ${task.name}")
            task.update(service,this)
        }

        if (taskStatus == Status.COMPLETE) {
            if (activeHit) {
                mturkAwsFacadeService.expire(service,activeHit)
                activeHit == null
            }
        }

        //TODO handle abort?

        if (taskStatus in Status.RUN_STATES && activeHit.age > taskProperties.relaunchInterval) {
            addActive(mturkAwsFacadeService.recycle(service,activeHit))
        }
        save()

    }

    def addActive(HitView hitView) {
        log.debug "AddActive: This identity: ${System.identityHashCode(this)} id: $id"
        activeHit = hitView
        addToAllHits(hitView)
        save()
    }



    def setStatus(Status s) {
        this.taskStatus = s
        if (taskStatus == Status.COMPLETE) {
            mturkTaskService.onTaskComplete(this)
        }
    }

    def String getSubmitUrl() {
         workflowRun.real? "https://www.mturk.com/mturk/externalSubmit" : "http://workersandbox.mturk.com/mturk/externalSubmit"
    }

    /**
     * Created by josh on 2/21/14.
     */
    public static enum Status {

        WAITING,RUNNING,ABORTED,NEEDS_INPUT,COMPLETE

        static Status[] RUN_STATES = [RUNNING,NEEDS_INPUT]

    }
}
