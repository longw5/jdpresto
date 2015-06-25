/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.execution;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;

import javax.validation.constraints.Min;

public class NodeSchedulerConfig
{
    private int minCandidates = 10;
    private boolean locationAwareScheduling = true;
    private boolean includeCoordinator = true;
    private boolean multipleTasksPerNode;
    private int maxSplitsPerNode = 100;
    private int maxPendingSplitsPerNodePerTask = 10;

    public static final String REPORT_NODE_DEFAULT = "NA";
    private String reportNodes = REPORT_NODE_DEFAULT;
    private boolean schedulerFixToReport = true;
    private boolean schedulerSourceToReport = true;

    public boolean isMultipleTasksPerNodeEnabled()
    {
        return multipleTasksPerNode;
    }

    @ConfigDescription("Allow nodes to be selected multiple times by the node scheduler, in a single stage")
    @Config("node-scheduler.multiple-tasks-per-node-enabled")
    public NodeSchedulerConfig setMultipleTasksPerNodeEnabled(boolean multipleTasksPerNode)
    {
        this.multipleTasksPerNode = multipleTasksPerNode;
        return this;
    }

    @Min(1)
    public int getMinCandidates()
    {
        return minCandidates;
    }

    @Config("node-scheduler.min-candidates")
    public NodeSchedulerConfig setMinCandidates(int candidates)
    {
        this.minCandidates = candidates;
        return this;
    }

    public boolean isLocationAwareSchedulingEnabled()
    {
        return locationAwareScheduling;
    }

    @Config("node-scheduler.location-aware-scheduling-enabled")
    public NodeSchedulerConfig setLocationAwareSchedulingEnabled(boolean locationAwareScheduling)
    {
        this.locationAwareScheduling = locationAwareScheduling;
        return this;
    }

    public boolean isIncludeCoordinator()
    {
        return includeCoordinator;
    }

    @Config("node-scheduler.include-coordinator")
    public NodeSchedulerConfig setIncludeCoordinator(boolean includeCoordinator)
    {
        this.includeCoordinator = includeCoordinator;
        return this;
    }

    @Config("node-scheduler.max-pending-splits-per-node-per-task")
    public NodeSchedulerConfig setMaxPendingSplitsPerNodePerTask(int maxPendingSplitsPerNodePerTask)
    {
        this.maxPendingSplitsPerNodePerTask = maxPendingSplitsPerNodePerTask;
        return this;
    }

    public int getMaxPendingSplitsPerNodePerTask()
    {
        return maxPendingSplitsPerNodePerTask;
    }

    public int getMaxSplitsPerNode()
    {
        return maxSplitsPerNode;
    }

    @Config("node-scheduler.max-splits-per-node")
    public NodeSchedulerConfig setMaxSplitsPerNode(int maxSplitsPerNode)
    {
        this.maxSplitsPerNode = maxSplitsPerNode;
        return this;
    }

    public String getReportNodes()
    {
        return reportNodes;
    }

    @Config("node-scheduler.report-nodes")
    public NodeSchedulerConfig setReportNodes(String reportNodes)
    {
        this.reportNodes = reportNodes;
        return this;
    }

    public boolean getSchedulerFixToReport()
    {
        return schedulerFixToReport;
    }

    @Config("node-scheduler.scheduler-fix-to-report")
    public NodeSchedulerConfig setSchedulerFixToReport(boolean schedulerFixToReport)
    {
        this.schedulerFixToReport = schedulerFixToReport;
        return this;
    }

    public boolean getSchedulerSourceToReport()
    {
        return schedulerSourceToReport;
    }

    @Config("node-scheduler.scheduler-source-to-report")
    public NodeSchedulerConfig setSchedulerSourceToReport(boolean schedulerSourceToReport)
    {
        this.schedulerSourceToReport = schedulerSourceToReport;
        return this;
    }
}
