package edu.msu.mi.turkmdr

import com.amazonaws.mturk.service.axis.RequesterService
import com.amazonaws.mturk.util.ClientConfig
import groovy.util.logging.Log4j

import javax.persistence.Transient

@Log4j
class WorkflowRun implements BeatListener {

    static constraints = {
    }


    static hasMany = [currentTasks: TaskRun, taskProperties: TaskProperties]
    static mappedBy = [taskProperties: "none"]


    static enum Status {
        WAITING, RUNNING, DONE
    }

    def mturkTaskService


   // RequesterService requesterService
    Set<TaskRun> currentTasks = [] as Set
    Status currentStatus
    boolean real
    Set<String> retriableErrors = ["Server.ServiceUnavailable"] as Set<String>
    int retryAttempts = 10
    long retryDelayMillis = 1000
    int iteration = 0
    int maxIterations = 0
    TaskProperties globalProperties
    Map taskProperties = [:]
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



    def run(times) {
        if (currentStatus != Status.WAITING) throw new MturkStateException("Can't reuse a workflow object; plese use 'copy' if you would like to run with existing parameters")
        currentStatus = Status.RUNNING
        maxIterations = times
        workflow.startingTasks.each { task ->
            TaskProperties p =  getTaskProperties(task)
            p.save()
            def tr = new TaskRun(task, p)
            addToCurrentTasks(tr)
            log.info("Added starting task ${task.name}")
        }
        save()

    }

    TaskProperties getTaskProperties(Task task) {
        TaskProperties taskP = taskProperties[task.name] ? workflow.allTasks[task.name].taskProperties.copyFrom(taskProperties[task.name]) : workflow.allTasks[task.name].taskProperties
        globalProperties.copyFrom(taskP)

    }


    @Override
    def beat(def Object beater, long timestamp) {
        def next = []
        if (currentStatus == Status.RUNNING) {
            currentTasks.each {
                it.beat(this, System.currentTimeMillis())
                if (it.status == TaskRun.Status.COMPLETE) {
                    currentTasks.remove(it)
                    next += it.task.next?:[]
                }
            }
        }
        currentTasks += next
        if (currentTasks.isEmpty()) {
            ++iteration
            mturkTaskService.onWorkflow(this)
            if (iteration >= maxIterations) {
                currentStatus = Status.DONE
            } else {
                currentTasks += workflow.startingTasks.collect { task ->
                    new TaskRun(task, getTaskProperties(task))
                }
            }
        }
        save()

    }


}
