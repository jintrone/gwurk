package edu.msu.mi.turkmdr

class TaskProperties implements Cloneable {


    static hasMany = [WorkflowRun]

    static constraints = {
        controller nullable: true
        action nullable: true
        rewardAmount nullable: true
        relaunchInterval nullable: true
        autoApprove nullable: true
        lifetime nullable: true
        assignmentDuration nullable: true
        keywords nullable: true
        maxAssignments nullable: true
        title nullable: true
        description nullable: true
        height nullable: true
        requireApproval nullable: true

    }



    Float rewardAmount
    Float relaunchInterval
    Boolean autoApprove
    Long lifetime
    Long assignmentDuration
    String keywords

    //Max assignments is the number of assignments *total* that we require; they may be split over individual hits
    Integer maxAssignments
    String title
    String description
    Integer height

    //Require approval determines if we require approval of assignments in order to advance the ball
    Boolean requireApproval
    String controller
    String action


    def TaskProperties copyFrom(TaskProperties props) {

        TaskProperties result = new TaskProperties(this.properties)
        if (result.id) {
            log.warn("Cloned result has an ID!!!! OH NO!!!")
        }
        props?.properties?.each {k,v->
           if (v) result[k] = v
        }
        result
    }


}
