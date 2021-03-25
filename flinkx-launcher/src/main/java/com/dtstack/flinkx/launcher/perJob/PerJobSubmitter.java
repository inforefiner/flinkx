/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.flinkx.launcher.perJob;

import com.dtstack.flinkx.launcher.YarnConfLoader;
import com.dtstack.flinkx.options.Options;
import com.dtstack.flinkx.util.MapUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.ClusterClientProvider;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.util.OptionalFailure;
import org.apache.flink.yarn.YarnClusterDescriptor;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

import static com.dtstack.flinkx.launcher.Launcher.*;
import static org.apache.hadoop.yarn.api.records.FinalApplicationStatus.*;

/**
 * Date: 2019/09/11
 * Company: www.dtstack.com
 * @author tudou
 */
public class PerJobSubmitter {
    private static final Logger LOG = LoggerFactory.getLogger(PerJobSubmitter.class);

    /**
     * submit per-job task
     * @param launcherOptions LauncherOptions
     * @param jobGraph JobGraph
     * @param remoteArgs remoteArgs
     * @return
     * @throws Exception
     */
    public static String submit(Options launcherOptions, JobGraph jobGraph, String[] remoteArgs) throws Exception{
        LOG.info("start to submit per-job task, launcherOptions = {}", launcherOptions.toString());
        Properties conProp = MapUtil.jsonStrToObject(launcherOptions.getConfProp(), Properties.class);
        ClusterSpecification clusterSpecification = FlinkPerJobUtil.createClusterSpecification(conProp);
        clusterSpecification.setCreateProgramDelay(true);

        String pluginRoot = launcherOptions.getPluginRoot();
        File jarFile = new File(pluginRoot + File.separator + getCoreJarFileName(pluginRoot));
        clusterSpecification.setConfiguration(launcherOptions.loadFlinkConfiguration());
        clusterSpecification.setClasspaths(analyzeUserClasspath(launcherOptions.getJob(), pluginRoot));
        clusterSpecification.setEntryPointClass(MAIN_CLASS);
        clusterSpecification.setJarFile(jarFile);

        if (StringUtils.isNotEmpty(launcherOptions.getS())) {
            clusterSpecification.setSpSetting(SavepointRestoreSettings.forPath(launcherOptions.getS()));
        }
        clusterSpecification.setProgramArgs(remoteArgs);
        clusterSpecification.setCreateProgramDelay(true);
        clusterSpecification.setYarnConfiguration(YarnConfLoader.getYarnConf(launcherOptions.getYarnconf()));
        PerJobClusterClientBuilder perJobClusterClientBuilder = new PerJobClusterClientBuilder();
        perJobClusterClientBuilder.init(launcherOptions, conProp);

        YarnClusterDescriptor descriptor = perJobClusterClientBuilder.createPerJobClusterDescriptor(launcherOptions);
        YarnClient yarnClient = descriptor.getYarnClient();
        ClusterClientProvider<ApplicationId> provider = descriptor.deployJobCluster(clusterSpecification, jobGraph, true);
        ClusterClient<ApplicationId> clusterClient = provider.getClusterClient();
        String applicationId = clusterClient.getClusterId().toString();
        String flinkJobId = jobGraph.getJobID().toString();
        Collection<JobStatusMessage> list = clusterClient.listJobs().get();
        LOG.info("job size:" + list.size());
        JobID jobId = list.iterator().next().getJobId();
        LOG.info("deploy per_job with appId: {}}, jobId: {}", applicationId, flinkJobId);
        ApplicationId appId = ConverterUtils.toApplicationId(applicationId);
        String finalStatus = null;
        Map<String, Object> metrics = new HashMap<>();
        while (true) {
            ApplicationReport applicationReport = yarnClient.getApplicationReport(appId);
            YarnApplicationState state = applicationReport.getYarnApplicationState();
            FinalApplicationStatus appFinalStatus = applicationReport.getFinalApplicationStatus();
            LOG.debug("The application {} status is {}", appId, state);
            if (YarnApplicationState.RUNNING == state) {
                try {
                    CompletableFuture<JobStatus> jobStatusFuture = clusterClient.getJobStatus(jobId);
                    JobStatus flinkJobStatus = jobStatusFuture.get();
                    LOG.debug("flink job status: " + flinkJobStatus);
                    if (flinkJobStatus.isTerminalState()) {
                        finalStatus = flinkJobStatus.name();
                        break;
                    } else {
                        CompletableFuture<Map<String, Object>> future = clusterClient.getAccumulators(jobId);
                        Map<String, Object> map = future.get();
                        for (Map.Entry<String, Object> entry : map.entrySet()) {
                            metrics.put(entry.getKey(), entry.getValue());
                        }
                    }
                } catch (Throwable e) {
                    LOG.warn("fetch flink job latest metrics error, maybe the job is stop, use cache metrics");
                }
            } else if (YarnApplicationState.FAILED == state ||
                    YarnApplicationState.KILLED == state ||
                    YarnApplicationState.FINISHED == state) {
                /*
                 * 结束状态应该获取finalStatus判断状态
                 * */
                switch (appFinalStatus) {
                    case FAILED:
                    case KILLED:
                        finalStatus = "FAILED";
                        break;
                    case SUCCEEDED:
                        finalStatus = "FINISHED";
                        break;
                }
                break;
            } else {
                LOG.warn("Ignore {} state {}", appId, state);
            }
            Thread.sleep(1000 * 10);
        }

        LOG.info("print metrics information starting......");
        for (Map.Entry<String, Object> entry : metrics.entrySet()) {
            LOG.info(entry.getKey() + ": " + entry.getValue());
        }
        LOG.info("print metrics information finished.");

        if ("FINISHED".equals(finalStatus)) {
            LOG.info("TASK DONE");
        }
        if ("FAILED".equals(finalStatus)) {
            LOG.info("TASK FAILED");
        }
        return applicationId;
    }
}