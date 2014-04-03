package edu.msu.mi.gwurk

import com.amazonaws.mturk.addon.HITProperties
import com.amazonaws.mturk.requester.QualificationRequirement

/**
 * Created by josh on 2/19/14.
 */
class MturkHitProperties extends HITProperties {

    MturkHitProperties() {
        super(new Properties())
    }

    MturkHitProperties(String propertyFile) throws IOException {
        super(propertyFile)
    }

    MturkHitProperties(Properties props) {
        super(props);
    }

    public String getAnnotation(String s) {
        try {
            return super.getAnnotation()    //To change body of overridden methods use File | Settings | File Templates.
        } catch (Exception ex) {
            return s

        }
    }


    public long getAssignmentDuration(long d) {
        try {
            return super.getAssignmentDuration();
            //To change body of overridden methods use File | Settings | File Templates.
        } catch (Exception ex) {
            return d;
        }
    }

    public long getLifetime(long d) {
        try {
            return super.getLifetime();    //To change body of overridden methods use File | Settings | File Templates.
        } catch (Exception ex) {
            return d;
        }
    }

    public long getAutoApprovalDelay(long d) {
        try {
            return super.getAutoApprovalDelay();
            //To change body of overridden methods use File | Settings | File Templates.
        } catch (Exception ex) {
            return d;
        }
    }

    public String getDescription(String d) {
        try {
            return super.getDescription();
            //To change body of overridden methods use File | Settings | File Templates.
        } catch (Exception ex) {
            return d;
        }
    }

    public String getKeywords(String d) {
        try {
            return super.getKeywords();    //To change body of overridden methods use File | Settings | File Templates.
        } catch (Exception ex) {
            return d;
        }
    }

    public int getMaxAssignments(int maxassignments) {
        try {
            return super.getMaxAssignments();
            //To change body of overridden methods use File | Settings | File Templates.
        } catch (Exception ex) {
            return maxassignments;
        }
    }

    public double getRewardAmount(double reward) {
        try {
            return super.getRewardAmount();
            //To change body of overridden methods use File | Settings | File Templates.
        } catch (Exception ex) {
            return reward;
        }
    }

    public String getTitle(String d) {
        try {
            return super.getTitle();    //To change body of overridden methods use File | Settings | File Templates.
        } catch (Exception ex) {
            return d;
        }
    }

    public QualificationRequirement[] getQualificationRequirements(QualificationRequirement[] reqs) {
        try {
            return super.getQualificationRequirements();
        } catch (Exception ex) {
            return reqs;
        }
    }


}

//}
