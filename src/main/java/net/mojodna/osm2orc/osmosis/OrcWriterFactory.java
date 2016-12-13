package net.mojodna.osm2orc.osmosis;

import org.openstreetmap.osmosis.core.pipeline.common.TaskConfiguration;
import org.openstreetmap.osmosis.core.pipeline.common.TaskManager;
import org.openstreetmap.osmosis.core.pipeline.common.TaskManagerFactory;
import org.openstreetmap.osmosis.core.pipeline.v0_6.SinkManager;

import java.util.logging.Logger;

public class OrcWriterFactory extends TaskManagerFactory {
    private static final String ARG_FILE_NAME = "file";
    private static final String DEFAULT_FILE_NAME = "dump.osm.orc";
    private static final Logger LOG = Logger.getLogger(OrcWriterFactory.class.getName());

    protected TaskManager createTaskManagerImpl(TaskConfiguration taskConfig) {
        String filename = getStringArgument(taskConfig, ARG_FILE_NAME,
                getDefaultStringArgument(taskConfig, DEFAULT_FILE_NAME));

        OrcWriter task = new OrcWriter(filename);

        return new SinkManager(taskConfig.getId(), task, taskConfig.getPipeArgs());
    }
}
