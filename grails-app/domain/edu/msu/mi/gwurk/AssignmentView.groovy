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
        data nullable: true


    }

    static mapping = {
        rawAnswer column: "raw_answer", sqlType: "char", length: 8192
        data sqlType: "blob"

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
    boolean hasData = false
    byte[] data


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
        this.hit = HitView.find { hitId == "${a.getHITId()}" }
        this.rawAnswer = a.answer
        hasData = getFileKey()!=null
        log.info("Has data? ${hasData}")
       save()
    }

    def update(RequesterService service) {
        def a = service.getAssignment(assignmentId).assignment
        switch (a.assignmentStatus) {
            case AssignmentStatus.Submitted:
                assignmentStatus = Status.SUBMITTED
                if (hit.taskRun.taskProperties.autoApprove) {
                    try {
                        service.approveAssignment(assignmentId, "Thanks for your help!")
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
            this.approvalTime = a.approvalTime?.time ?: this.approvalTime

        } else if (assignmentStatus == Status.REJECTED) {
            this.rejectTime = a.rejectionTime?.time ?: this.rejectTime
        }

        this.requestorFeedback = a?.requesterFeedback

        if (hasData && !data) {
            log.info("Retrieve data from service")
            retrieveFile(service)
        }

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
            if (a.freeText) {
                [a.getQuestionIdentifier(), a.freeText]
            } else if (a.selectionIdentifier) {
                [a.getQuestionIdentifier(), a.selectionIdentifier]
            } else if (a.otherSelectionText) {
                [a.questionIdentifier, a.otherSelectionText]
            } else if (a.uploadedFileSizeInBytes) {
                [a.questionIdentifier, [size: a.uploadedFileSizeInBytes, key: a.uploadedFileKey]]
            }
        }

    }

    private Map.Entry getFileKey() {
        getAnswer().find { k, v ->
            return (v.hasProperty("size") && v.hasProperty("key"))
        }
    }

    private retrieveFile(RequesterService service) {
        Map.Entry ent = getFileKey()
        log.info("Retrieving file data ${ent.value.size}")
        String s = service.getFileUploadURL(assignmentId, ent.key)
        data = new byte[ent.value.size as int];
        DataInputStream dataIs = new DataInputStream(new URL(s).openConnection().inputStream)
        dataIs.readFully(data);
        dataIs.close()

    }


}
