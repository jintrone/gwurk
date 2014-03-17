package edu.msu.mi.turkmdr

import com.amazonaws.mturk.service.exception.InternalServiceException
import grails.transaction.Transactional
import groovy.util.logging.Log4j

@Log4j
@Transactional
class MturkMonitorService {

    Timer heartBeat;
    boolean running
    long pauseTime = 60000l
    def listeners = [] as Set<BeatListener>

    def init() {
        restart()
    }

    def halt() {
        if (running) {
            heartBeat.cancel()
        }
    }



    public void cleanup() {
        heartBeat = null
        log.info("Would be cleaning up")
    }

    def restart() {
        if (running) {
            log.warn("Timer is already running; please use halt to stop if you wish to restart");
            return;

        }

        heartBeat = new Timer() {
            public void cancel() {
                super.cancel();
                running = false;
                cleanup();
            }
        }

        heartBeat.schedule(new TimerTask() {

            public boolean cancel() {
                boolean result = super.cancel();
                heartBeat.cancel();
                return result;

            }

            @Override
            public void run() {
                running = true;
                try {
                    beat();


                } catch (InternalServiceException e1) {
                    log.warn("AWS Service Exception: ");
                    e1.printStackTrace();
                    log.warn("Continuing");

                } catch (Exception e) {
                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                    heartBeat.cancel();
                }


            }
        }, 0, pauseTime);

    }

    def launch(Workflow w, boolean real, int iterations, Credentials credentials,Map props) {
        WorkflowRun run = new WorkflowRun(w,credentials,real,props)
        run.save()
        run.run(iterations)
        listeners.add(run)
    }

    def beat() {
        log.info "Dropping the beat"
        listeners.each {
            if (it instanceof WorkflowRun) {
                it = WorkflowRun.get(it.id)
            }
            it.beat(this,System.currentTimeMillis())
        }
        listeners.removeAll {
            it.hasProperty("currentStatus") && it.currentStatus == TaskRun.Status.COMPLETE

        }
    }
}
