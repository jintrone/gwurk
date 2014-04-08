package edu.msu.mi.gwurk

import com.amazonaws.mturk.service.axis.RequesterService
import com.amazonaws.mturk.util.ClientConfig
import groovy.util.logging.Log4j

@Log4j
class WorkflowRun implements BeatListener {

    static constraints = {
    }


    static hasMany = [currentTasks: TaskRun, allTasks: TaskRun, taskProperties: TaskProperties]
    static mappedBy = [taskProperties: "none", currentTasks: "activeWorkflowRun", allTasks: "workflowRun"]


    static enum Status {
        WAITING,  RUNNING, DONE
    }

    def mturkTaskService


   // RequesterService requesterService
    Set<TaskRun> currentTasks = [] as Set
    Set<TaskRun> allTasks = [] as Set
    Status currentStatus
    boolean real
    Set<String> retriableErrors = ["Server.ServiceUnavailable"] as Set<String>
    int retryAttempts = 10
    long retryDelayMillis = 1000
    int iteration = 0
    int maxIterations = 0

    TaskProperties globalProperties
    Map taskProperties = [:]
    Map<String,String> userProperties = [:]

    Workflow workflow
    Credentials credentials

    WorkflowRun(Workflow w, Credentials credentials, boolean real, Map props) {
        workflow = w
        this.real = real
        this.credentials = credentials
        globalProperties = w.taskProperties

        props.each { k, v ->
            if (v instanceof Map) {
                if (k == "global") {
                    globalProperties = globalProperties.copyFrom(new TaskProperties(v))

                } else {
                    taskProperties.put(k, new TaskProperties(v).save())
                }
            }
        }
        globalProperties.save()
        currentStatus = Status.WAITING

    }



    RequesterService getRequesterService() {
        ClientConfig config = new ClientConfig()
        config.setAccessKeyId(credentials.awsId)
        config.setSecretAccessKey(credentials.awsSecret)
        config.setRetriableErrors(retriableErrors)
        config.setRetryAttempts(retryAttempts)
        config.setRetryDelayMillis(retryDelayMillis)
        if (real) {
            config.setServiceURL(ClientConfig.PRODUCTION_SERVICE_URL);

        } else {
            config.setServiceURL(ClientConfig.SANDBOX_SERVICE_URL);

        }
        new RequesterService(config)
    }

    TaskRun addTask(Task t, boolean current, TaskRun... previous) {
        TaskProperties p =  getTaskProperties(t)
        p.save()
        def tr = new TaskRun(t, p)
        addToAllTasks(tr)
        if (current) addToCurrentTasks(tr)
        if (previous) previous.each {
            tr.addToPreviousTaskRuns(it)
        }
        save()
        tr
    }

    def run(times) {
        if (currentStatus != Status.WAITING) throw new MturkStateException("Can't reuse a workflow object; plese use 'copy' if you would like to run with existing parameters")
        currentStatus = Status.RUNNING
        maxIterations = times
         kickoff()

        save()

    }

    def kickoff() {
        workflow.startingTasks.each { task ->
            addTask(task,true)
        }
    }

    TaskProperties getTaskProperties(Task task) {
        TaskProperties taskP = taskProperties[task.name] ? workflow.allTasks[task.name].taskProperties.copyFrom(taskProperties[task.name]) : workflow.allTasks[task.name].taskProperties
        globalProperties.copyFrom(taskP)

    }

    void setUserProperty(String key, String val) {
        userProperties[key] = val
        save()
    }

    String getUserProperty(String key) {
        userProperties[key]
    }


    @Override
    def beat(def Object beater, long timestamp) {
        def next = []
        if (currentStatus == Status.RUNNING) {
            currentTasks.toArray(new TaskRun[0]).each { TaskRun currTaskRun ->

                currTaskRun.beat(this, System.currentTimeMillis())
                if (currTaskRun.taskStatus == TaskRun.Status.COMPLETE) {
                    log.info("Removing TaskRun:${currTaskRun.task.name}")
                    removeFromCurrentTasks(currTaskRun)
                    if (currTaskRun.task.next) {
                        currTaskRun.task.next.each {
                            if (mturkTaskService.advanceToTask(currTaskRun,it)) {
                                addTask(it,true,currTaskRun)
                            } else {
                                log.info("Could not advance to ${it.name}; fails conditional")
                            }

                        }

                    }
                }

            }

        }

        save()
        log.info("Current tasks after save are now $currentTasks")
        if (currentTasks.isEmpty()) {
            ++iteration
            mturkTaskService.onWorkflow(this)
            if (iteration >= maxIterations) {
                currentStatus = Status.DONE
            } else {
               kickoff()
            }
        }
        save()

    }


}
