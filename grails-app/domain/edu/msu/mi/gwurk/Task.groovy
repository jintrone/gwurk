package edu.msu.mi.gwurk

import com.amazonaws.mturk.service.axis.RequesterService
import groovy.util.logging.Log4j

@Log4j
abstract class Task  {

    static constraints = {
        workflow nullable: true
    }

    static hasMany = [next:Task]
    static belongsTo = [workflow:Workflow]

    static transients = ['mturkTaskService','mturkAwsFacadeService']


    abstract def start(RequesterService service, TaskRun runner)
    abstract def update(RequesterService service, TaskRun runner)

    String name
    def mturkAwsFacadeService
    def mturkTaskService

    TaskProperties taskProperties

    Task(String name, Map map)  {
        this.name = name
        this.taskProperties = new TaskProperties(map)
        this.taskProperties.save()
    }



    def addNextTask(Task t) {
        addToNext(t)

    }



    MturkTaskService getService() {
        return mturkTaskService
    }




}
