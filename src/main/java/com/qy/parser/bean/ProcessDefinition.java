/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.qy.parser.bean;


import java.util.Date;


/**
 * process definition
 */

public class ProcessDefinition {
    /**
     * id
     */
    private int id;

    /**
     * name
     */
    private String name;

    /**
     * version
     */
    private int version;



    /**
     * project id
     */
    private int projectId;

    /**
     * definition json string
     */
    private String processDefinitionJson;

    /**
     * description
     */
    private String description;

    /**
     * user defined parameters
     */
    private String globalParams;


    /**
     * create time
     */
    private Date createTime;

    /**
     * update time
     */
    private Date updateTime;


    /**
     * project name
     */
    private String projectName;

    /**
     * locations array for web
     */
    private String locations;

    /**
     * connects array for web
     */
    private String connects;

    /**
     * receivers
     */
    private String receivers;

    /**
     * receivers cc
     */
    private String receiversCc;

    private String leadMobile;

    private int ddAlarmId;



    /**
     * process warning time out. unit: minute
     */
    private int timeout;

    /**
     * tenant id
     */
    private int tenantId;


    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }


    public String getProcessDefinitionJson() {
        return processDefinitionJson;
    }

    public void setProcessDefinitionJson(String processDefinitionJson) {
        this.processDefinitionJson = processDefinitionJson;
    }

    public Date getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }

    public int getProjectId() {
        return projectId;
    }

    public void setProjectId(int projectId) {
        this.projectId = projectId;
    }

    public Date getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(Date updateTime) {
        this.updateTime = updateTime;
    }



    public String getProjectName() {
        return projectName;
    }

    public void setProjectName(String projectName) {
        this.projectName = projectName;
    }


    public String getGlobalParams() {
        return globalParams;
    }






    public String getLocations() {
        return locations;
    }

    public void setLocations(String locations) {
        this.locations = locations;
    }

    public String getConnects() {
        return connects;
    }

    public void setConnects(String connects) {
        this.connects = connects;
    }

    public String getReceivers() {
        return receivers;
    }

    public void setReceivers(String receivers) {
        this.receivers = receivers;
    }

    public String getReceiversCc() {
        return receiversCc;
    }

    public void setReceiversCc(String receiversCc) {
        this.receiversCc = receiversCc;
    }

    public String getLeadMobile() {
        return leadMobile;
    }

    public void setLeadMobile(String leadMobile) {
        this.leadMobile = leadMobile;
    }

    public int getDdAlarmId() {
        return ddAlarmId;
    }

    public void setDdAlarmId(int ddAlarmId) {
        this.ddAlarmId = ddAlarmId;
    }



    public int getTimeout() {
        return timeout;
    }

    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }

    @Override
    public String toString() {
        return "ProcessDefinition{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", version=" + version +
                ", projectId=" + projectId +
                ", processDefinitionJson='" + processDefinitionJson + '\'' +
                ", globalParams='" + globalParams + '\'' +

                ", createTime=" + createTime +
                ", updateTime=" + updateTime +

                ", projectName='" + projectName + '\'' +
                ", locations='" + locations + '\'' +
                ", connects='" + connects + '\'' +
                ", receivers='" + receivers + '\'' +
                ", receiversCc='" + receiversCc + '\'' +
                ", timeout=" + timeout +
                ", tenantId=" + tenantId +
                '}';
    }

    public int getTenantId() {
        return tenantId;
    }

    public void setTenantId(int tenantId) {
        this.tenantId = tenantId;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }
}
