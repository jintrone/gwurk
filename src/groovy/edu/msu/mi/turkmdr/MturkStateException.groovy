package edu.msu.mi.turkmdr

import org.codehaus.groovy.GroovyException

/**
 * Created by josh on 2/19/14.
 */
class MturkStateException extends GroovyException {

    public MturkStateException(String msg) {
        super(msg, false)
    }
}
