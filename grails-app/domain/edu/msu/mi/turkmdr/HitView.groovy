package edu.msu.mi.turkmdr

import com.amazonaws.mturk.requester.Assignment
import com.amazonaws.mturk.requester.HIT
import com.amazonaws.mturk.requester.HITStatus
import com.amazonaws.mturk.service.axis.RequesterService
import com.amazonaws.mturk.service.axis.RequesterServiceRaw
import groovy.util.logging.Log4j

@Log4j
class HitView {

    static constraints = {

    }

    static belongsTo = [taskRun:TaskRun]
    static hasMany = [assignments:AssignmentView]

    static enum Status {
        AVAILABLE, UNAVAILABLE, REVIEWABLE, FINISHED
    }



    String hitId
    String hitGroup
    Date creationTime
    Status status

    HitView(TaskRun run, HIT hit) {
        hitId = hit.HITId
        hitGroup = hit.HITGroupId
        creationTime = hit.creationTime.getTime()
        status = Status.AVAILABLE
        taskRun = run
        save()
    }

    long getAge() {
        System.currentTimeMillis() - creationTime.time
    }



    def update(RequesterService requesterService) {
        HIT h = requesterService.getHIT(hitId)

        switch(h.getHITStatus()) {

            case HITStatus.Assignable:
                status = Status.AVAILABLE
                break
            case HITStatus.Unassignable:
                status= Status.UNAVAILABLE
                break
            case HITStatus.Reviewable:
                status = Status.REVIEWABLE
                break
            case HITStatus.Disposed:
                status = Status.FINISHED
        }
        save()
        def known = assignments*.assignmentId as Set
        def awsAssts = requesterService.getAllAssignmentsForHIT(hitId)
        log.info "Retrieved assignments from service: ${awsAssts}"
        awsAssts.findAll {it && !known.contains(it.assignmentId)}.each {
            log.info("Adding assignment $it")
            addToAssignments(new AssignmentView(it))
        }
        assignments.each {
           if (it.status == AssignmentView.Status.SUBMITTED) it.update(requesterService)
        }
        save()

    }

    AssignmentView[] getSubmittedAssignments() {
        return assignments.findAll {it.status == AssignmentView.Status.SUBMITTED}
    }

    AssignmentView[] getReviewedAssignments() {
        return assignments.findAll {it.status != AssignmentView.Status.SUBMITTED}
    }

    AssignmentView[] getRejectedAssignments() {
        return assignments.findAll {it.status != AssignmentView.Status.REJECTED}
    }

    AssignmentView[] getApprovedAssignments() {
        return assignments.findAll {it.status != AssignmentView.Status.APPROVED}
    }

    def expire(RequesterService requesterService) {
        requesterService.forceExpireHIT(hitId)
        update(requesterService)
        save()
    }

}
