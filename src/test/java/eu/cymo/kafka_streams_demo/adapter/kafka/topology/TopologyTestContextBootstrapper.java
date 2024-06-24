package eu.cymo.kafka_streams_demo.adapter.kafka.topology;

import org.springframework.boot.test.context.SpringBootTestContextBootstrapper;
import org.springframework.test.context.MergedContextConfiguration;
import org.springframework.test.context.TestContextAnnotationUtils;

public class TopologyTestContextBootstrapper extends SpringBootTestContextBootstrapper {

    @Override
    protected MergedContextConfiguration processMergedContextConfiguration(MergedContextConfiguration mergedConfig) {
        return super.processMergedContextConfiguration(mergedConfig);
    }

    @Override
    protected String[] getProperties(Class<?> testClass) {
        TopologyTest topologyTest = TestContextAnnotationUtils.findMergedAnnotation(testClass, TopologyTest.class);
        return (topologyTest != null) ? topologyTest.properties() : null;
    }
    
}
