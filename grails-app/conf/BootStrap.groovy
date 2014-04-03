import edu.msu.mi.gwurk.MturkMonitorService

/**
 * Created by josh on 2/19/14.
 */

class BootStrap {

    MturkMonitorService mturkMonitorService


    def init = { servletContext ->
        mturkMonitorService.init()

    }


}