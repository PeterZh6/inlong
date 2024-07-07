/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.inlong.manager.pojo.audit;

/**
 * Audit Data Scale Request
 */
public class AuditDataScaleRequest {

    private String startTime;
    private String endTime;
    private String inlongGroupId;
    private String inlongStreamId;
    private String auditId;
    private String auditCycle;

    /**
     * Audit Data Scale Request
     * @param startTime
     * @param endTime
     * @param inlongGroupId
     * @param inlongStreamId
     * @param auditId
     * @param auditCycle
     */
    public AuditDataScaleRequest(String startTime, String endTime, String inlongGroupId, String inlongStreamId,
            String auditId, String auditCycle) {
        this.startTime = startTime;
        this.endTime = endTime;
        this.inlongGroupId = inlongGroupId;
        this.inlongStreamId = inlongStreamId;
        this.auditId = auditId;
        this.auditCycle = auditCycle;
    }

    public String getStartTime() {
        return startTime;
    }

    public void setStartTime(String startTime) {
        this.startTime = startTime;
    }

    public String getEndTime() {
        return endTime;
    }

    public void setEndTime(String endTime) {
        this.endTime = endTime;
    }

    public String getInlongGroupId() {
        return inlongGroupId;
    }

    public void setInlongGroupId(String inlongGroupId) {
        this.inlongGroupId = inlongGroupId;
    }

    public String getInlongStreamId() {
        return inlongStreamId;
    }

    public void setInlongStreamId(String inlongStreamId) {
        this.inlongStreamId = inlongStreamId;
    }

    public String getAuditId() {
        return auditId;
    }

    public void setAuditId(String auditId) {
        this.auditId = auditId;
    }

    public String getAuditCycle() {
        return auditCycle;
    }

    public void setAuditCycle(String auditCycle) {
        this.auditCycle = auditCycle;
    }
}
