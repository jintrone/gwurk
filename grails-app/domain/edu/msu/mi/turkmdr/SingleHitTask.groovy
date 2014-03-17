package edu.msu.mi.turkmdr


import com.amazonaws.mturk.service.axis.RequesterService
import groovy.util.logging.Log4j

@Log4j
class SingleHitTask extends Task {

    static constraints = {
    }


    SingleHitTask(String name, Map props) {
        super(name, props)
    }

    @Override
    def start(RequesterService service, TaskRun runner) {
        mturkAwsFacadeService.launchHit(service, runner)
    }


    @Override
    def update(RequesterService service, TaskRun runner) {

        mturkAwsFacadeService.refresh(service, runner)
        log.info "All assignments ${runner.allHits*.assignments}"
        runner.allHits*.assignments.flatten().each { AssignmentView av ->
            if (!av) {
                log.info("Why is there a null entity?")
            } else {
                if (!av.processed) {
                    if (!runner.taskProperties.requireApproval || av.status == AssignmentView.Status.APPROVED) {
                        mturkTaskService.onAssignment(runner,av)
                        av.processed = true
                    }
                }
            }
        }
        if (runner.hasAllAssignments() || runner.status == TaskRun.Status.ABORTED) {
            mturkTaskService.onHit(runner,runner.activeHit)
            runner.status = TaskRun.Status.COMPLETE
        } else if (runner.hasPendingAssignments()) {
            runner.status = TaskRun.Status.NEEDS_INPUT
        }

        save()


    }
}
