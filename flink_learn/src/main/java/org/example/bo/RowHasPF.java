package org.example.bo;

import java.io.Serializable;
import java.util.Objects;

public class RowHasPF implements Serializable {

    private String jd;

    /**
     * yyyy-MM-dd形式
     */
    private String ptDT;

    private long eventTime;

    public String getJd() {
        return jd;
    }

    public void setJd(String jd) {
        this.jd = jd;
    }

    public String getPtDT() {
        return ptDT;
    }

    public void setPtDT(String ptDT) {
        this.ptDT = ptDT;
    }

    public long getEventTime() {
        return eventTime;
    }

    public void setEventTime(long eventTime) {
        this.eventTime = eventTime;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RowHasPF rowHasPF = (RowHasPF) o;
        return eventTime == rowHasPF.eventTime &&
                Objects.equals(jd, rowHasPF.jd) &&
                Objects.equals(ptDT, rowHasPF.ptDT);
    }

    @Override
    public int hashCode() {
        return Objects.hash(jd, ptDT, eventTime);
    }
}
