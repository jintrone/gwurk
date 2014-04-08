package edu.msu.mi.gwurk

import com.amazonaws.mturk.dataschema.QuestionFormAnswers
import com.amazonaws.mturk.dataschema.QuestionFormAnswersType
import com.amazonaws.mturk.requester.Assignment
import com.amazonaws.mturk.requester.AssignmentStatus
import com.amazonaws.mturk.service.axis.RequesterService
import com.amazonaws.mturk.service.exception.ServiceException


class AssignmentView {

    static constraints = {
        approvalTime nullable: true
        rejectTime nullable: true
        requestorFeedback nullable: true

    }

    static mapping = {
        rawAnswer column: "raw_answer", sqlType: "char", length: 4096
    }
    static enum Status {
        SUBMITTED, APPROVED, REJECTED
    }



    static belongsTo = [hit: HitView]

    boolean processed
    Status assignmentStatus
    String assignmentId
    String rawAnswer
    Date submitTime
    Date acceptTime
    String workerId


    Date approvalTime
    Date rejectTime
    String requestorFeedback


    public AssignmentView(Assignment a) {
        this.assignmentStatus = Status.SUBMITTED
        this.processed = false
        this.assignmentId = a.assignmentId
        this.workerId = a.workerId
        this.acceptTime = a.acceptTime.getTime()
        this.submitTime = a.submitTime.getTime()
        this.hit = HitView.find { hitId=="${a.getHITId()}"}
        this.rawAnswer = a.answer

        save()
    }

    def update(RequesterService service) {
        def a = service.getAssignment(assignmentId).assignment
        switch (a.assignmentStatus) {
            case AssignmentStatus.Submitted:
                assignmentStatus = Status.SUBMITTED
                if (hit.taskRun.taskProperties.autoApprove) {
                    try {
                        service.approveAssignment(assignmentId,"Thanks for your help!")
                        this.approvalTime = new Date()
                    update(service)
                    } catch (ServiceException ex) {
                        log.warn("Service exception while approving hit: ${ex}")
                    }
                }
                break

            case AssignmentStatus.Approved:
                assignmentStatus = Status.APPROVED
                break

            case AssignmentStatus.Rejected:
                assignmentStatus = Status.REJECTED
                break
        }
        if (assignmentStatus == Status.APPROVED) {
            this.approvalTime = a.approvalTime?.time?:this.approvalTime

        } else if (assignmentStatus == Status.REJECTED) {
            this.rejectTime = a.rejectionTime?.time?:this.rejectTime
        }

        this.requestorFeedback = a?.requesterFeedback
        save()
        if (hasErrors()) {
            log.warn(errors)
        }

    }

    public Map getAnswer() {
        return extractAnswers(rawAnswer)
    }

    public static Map extractAnswers(String answer) {
        QuestionFormAnswers answers = RequesterService.parseAnswers(answer);
        answers.answer.collectEntries { QuestionFormAnswersType.AnswerType a ->
            [a.getQuestionIdentifier(),a.freeText]
        }

    }

}
