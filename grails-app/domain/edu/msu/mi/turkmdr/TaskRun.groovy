package edu.msu.mi.turkmdr

import com.amazonaws.mturk.service.axis.RequesterService
import groovy.util.logging.Log4j

@Log4j
class TaskRun implements BeatListener{

    static constraints = {
        activeHit nullable: true
        allHits nullable: true

    }




    static belongsTo = [workflowRun:WorkflowRun]
    static hasMany = [allHits:HitView]
    static mappedBy = [allHits:"taskRun"]

    Status status
    HitView activeHit
    TaskProperties taskProperties
    Task task


    public TaskRun(Task task, TaskProperties props) {
        this.task = task;
        this.taskProperties = props
        this.status = Status.WAITING
        log.info "Newly created: ${System.identityHashCode(this)} id: $id"
    }

    def mturkAwsFacadeService
    def mturkTaskService

    /**
     * Checks if there are enough assignments to complete this task
     * @return
     */
    boolean hasAllAssignments() {
        log.info("Searching for all assignments")
        allHits*.assignments.findAll {
            println "Looking for assignment ${it}"
            taskProperties.requireApproval?it.status == AssignmentView.Status.APPROVED:true
        }.size() >= taskProperties.maxAssignments
    }

    /**
     * Checks if there are pending assignments
     * @return
     */
    boolean hasPendingAssignments() {
        allHits*.assignments.findAll {
            it.status == AssignmentView.Status.SUBMITTED
        }.size() > 0
    }

    def afterLoad() {
        log.info "After load: This identity: ${System.identityHashCode(this)} id: $id"
    }

    def afterInsert() {
        log.info "After insert: This identity: ${System.identityHashCode(this)} id: $id"
    }


    @Override
    def beat(def beater, long timestamp) {
        RequesterService service = (beater as WorkflowRun).requesterService
        if (status == Status.WAITING) {
            status = Status.RUNNING
            log.info "BEAT: This identity: ${System.identityHashCode(this)} id: $id"
            task.start(service,this)
        } else if (status in Status.RUN_STATES) {
            task.update(service,this)
        }

        if (status == Status.COMPLETE) {
            if (activeHit) {
                mturkAwsFacadeService.expire(service,activeHit)
                activeHit == null
            }
        }

        //TODO handle abort?

        if (status in Status.RUN_STATES && activeHit.age > taskProperties.relaunchInterval) {
            addActive(mturkAwsFacadeService.recycle(service,activeHit))
        }
        save()

    }

    def addActive(HitView hitView) {
        log.info "AddActive: This identity: ${System.identityHashCode(this)} id: $id"
        activeHit = hitView
        addToAllHits(hitView)
        save()
    }



    def setStatus(Status s) {
        this.status = s
        if (status == Status.COMPLETE) {
            mturkTaskService.onTask(this)
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
