package edu.msu.mi.turkmdr

/**
 * Created by josh on 3/10/14.
 */
class TaskPropertiesTests extends GroovyTestCase {



    public void testClone() {
        println "Starting test!"
        def m = [
                controller: "demographics",
                action: "create",
                rewardAmount: 0.03f,
                relaunchInterval: 1000 * 60 * 60,
                autoApprove: true,
                lifetime: 60 * 60 * 10,
                assignmentDuration: 60,
                keywords: "survey, demographics, research",
                maxAssignments: 100,
                title: "Enter some demographic information",
                description: "This a survey to understand the turker workforce a little better. No identifying information will be captured, and the data is for research purposes only",
                height: 1000,
                requireApproval: true]

        def n = [controller: "test"]

        TaskProperties p = new TaskProperties(m)
        assert p.controller == "demographics"
        p.save()

        TaskProperties p1 = p.clone()
        assert p1.id == p.id
        p1.save()
        assert p1.id != p.id



        assert p1.controller == p.controller

        TaskProperties p2 = new TaskProperties(n)
        TaskProperties p3 = p.copyFrom(p2)
        println "hi there"
        assert p3.controller == "test"






    }



}
