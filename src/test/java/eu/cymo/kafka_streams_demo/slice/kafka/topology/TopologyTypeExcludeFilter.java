package eu.cymo.kafka_streams_demo.slice.kafka.topology;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.springframework.boot.test.autoconfigure.filter.StandardAnnotationCustomizableTypeExcludeFilter;
import org.springframework.core.type.classreading.MetadataReader;
import org.springframework.core.type.classreading.MetadataReaderFactory;

import eu.cymo.kafka_streams_demo.adapter.kafka.KafkaConfig;
import eu.cymo.kafka_streams_demo.adapter.kafka.KafkaPropertiesAvroSerdeFactory;

public class TopologyTypeExcludeFilter extends StandardAnnotationCustomizableTypeExcludeFilter<TopologyTest> {
    private final List<String> EXCLUSION_LIST = Arrays.asList(
                KafkaConfig.class,
                KafkaPropertiesAvroSerdeFactory.class)
            .stream()
            .map(Class::getName)
            .toList();
    
    protected TopologyTypeExcludeFilter(Class<TopologyTest> testClass) {
        super(testClass);
    }
    
    @Override
    protected boolean exclude(MetadataReader metadataReader, MetadataReaderFactory metadataReaderFactory) throws IOException {
        return isInExclusionList(metadataReader) || super.exclude(metadataReader, metadataReaderFactory);
    }
    
    private boolean isInExclusionList(MetadataReader metadataReader) {
        return EXCLUSION_LIST.contains(metadataReader.getClassMetadata().getClassName());
    }
    
}
