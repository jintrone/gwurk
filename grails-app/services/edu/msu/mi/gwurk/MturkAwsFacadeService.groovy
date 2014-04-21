package edu.msu.mi.gwurk

import com.amazonaws.mturk.requester.HIT
import com.amazonaws.mturk.requester.QualificationRequirement
import com.amazonaws.mturk.service.axis.RequesterService
import grails.transaction.Transactional
import groovy.util.logging.Log4j
import org.codehaus.groovy.grails.web.mapping.LinkGenerator

@Log4j
@Transactional
class MturkAwsFacadeService {

    def grailsApplication
    LinkGenerator grailsLinkGenerator

    HitView recycle(RequesterService requesterService, HitView hitView) {
        expire(requesterService,hitView)
        launchHit(requesterService,hitView.taskRun)
    }

    def expire(RequesterService requesterService, HitView hitView) {
        hitView.expire(requesterService)
        hitView.taskRun.activeHit = null
        hitView.taskRun.save()
    }

    def refresh(RequesterService requesterService, TaskRun taskRun) {
        taskRun.activeHit.update(requesterService)
    }

    HitView launchHit(RequesterService requesterService, TaskRun taskRun) {
        taskRun.attach()
        MturkHitProperties props = getProperties(taskRun.taskProperties)
        log.info("Launch hit with taskrun id: "+taskRun.id)
        String url = "https://${grailsApplication.config.gwurk.hostname}:${grailsApplication.config.gwurk.port}${grailsLinkGenerator.link(action:"external",controller: "workflow", params:[task:taskRun.id])}"
        log.info("Would link: ${url}")
        HIT h = requesterService.createHIT(
                null, // hitTypeId
                props.getTitle("No title"),
                props.getDescription("No description"),
                props.getKeywords(null), // keywords
                getExternalQuestion(url, taskRun.taskProperties.height),
                props.getRewardAmount(0),
                props.getAssignmentDuration(60 * 5),
                props.getAutoApprovalDelay(60 * 30),
                taskRun.taskProperties.lifetime,
                props.getMaxAssignments(1),
                "", // requesterAnnotation
                props.getQualificationRequirements(new QualificationRequirement[0]), // qualificationRequirements
                (String[])["Minimal", "HITDetail", "HITQuestion", "HITAssignmentSummary"] as String[], // responseGroup
                null, // uniqueRequestToken
                null, // assignmentReviewPolicy
                null); // hitReviewPolicy
        HitView result = new HitView(taskRun,h)
        result.save()
        taskRun.addActive(result)
        result

    }


    static MturkHitProperties getProperties(TaskProperties properties) {
        def props = new MturkHitProperties()
        props.title = properties.title
        props.description = properties.description
        props.autoApprovalDelay = properties.autoApprove?0:24*60*60
        props.keywords = properties.keywords
        props.maxAssignments = properties.maxAssignments
        props.assignmentDuration = properties.assignmentDuration
        props.rewardAmount = properties.rewardAmount
        props.lifetime = properties.lifetime as String
        props
    }

    static String getExternalQuestion(String url,int frameHeight) {
        String question = String.format("<ExternalQuestion xmlns=\"http://mechanicalturk.amazonaws.com/AWSMechanicalTurkDataSchemas/2006-07-14/ExternalQuestion.xsd\">" +
                "<ExternalURL>%s</ExternalURL>" +
                "<FrameHeight>%d</FrameHeight>" +
                "</ExternalQuestion>", url,frameHeight);
        log.info("Attempt to launch external hit: "+question);
        return question;
    }

    BufferedOutputStream getExternalFile() {

    }


}
