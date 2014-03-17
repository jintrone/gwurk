package edu.msu.mi.turkmdr

import groovy.util.logging.Log4j

@Log4j
class CredentialsController {

    def index() {

       render(view:"index")
    }

    def add() {
        new Credentials(params).save()
        redirect(action:"index")
    }
}
