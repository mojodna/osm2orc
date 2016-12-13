package net.mojodna.osm2orc;

import org.openstreetmap.osmosis.core.pipeline.common.TaskManagerFactory;
import org.openstreetmap.osmosis.core.plugin.PluginLoader;

import java.util.HashMap;
import java.util.Map;

public class OrcPluginLoader implements PluginLoader {
    public Map<String, TaskManagerFactory> loadTaskFactories() {
        Map<String, TaskManagerFactory> factoryMap = new HashMap<>();

        OrcWriterFactory writer = new OrcWriterFactory();

        factoryMap.put("write-orc", writer);

        return factoryMap;
    }
}
