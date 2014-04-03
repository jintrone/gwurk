package edu.msu.mi.gwurk

import edu.msu.mi.gwurk.Credentials
import groovy.util.logging.Log4j

@Log4j
class CredentialsController {



    def index() {

    }

    def add() {
        new Credentials(params).save()
        redirect(action:"index",controller:"credentials")
    }
}
