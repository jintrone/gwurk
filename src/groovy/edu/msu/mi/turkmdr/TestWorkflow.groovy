package edu.msu.mi.turkmdr

/**
 * Created by josh on 2/19/14.
 */
class TestWorkflow {


    TestWorkflow() {


        Task one = new SingleHitTask("recipe","http://google.com", { result ->

        })
        Workflow w = new Workflow(one)
        w.addToTasks(one)
        w.save()

        new WorkflowRun(w,
            [global: [title:"Find a recipe",
                     description:"In this HIT, you will be asked to enter your favorite recipe"],
             (one.id): [maxAssignments]]).save()



    }
}
