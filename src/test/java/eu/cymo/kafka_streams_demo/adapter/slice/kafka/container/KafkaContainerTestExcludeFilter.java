package eu.cymo.kafka_streams_demo.adapter.slice.kafka.container;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.springframework.boot.test.autoconfigure.filter.StandardAnnotationCustomizableTypeExcludeFilter;
import org.springframework.core.type.classreading.MetadataReader;
import org.springframework.core.type.classreading.MetadataReaderFactory;

import eu.cymo.kafka_streams_demo.adapter.kafka.KafkaPropertiesAvroSerdeFactory;

public class KafkaContainerTestExcludeFilter extends StandardAnnotationCustomizableTypeExcludeFilter<KafkaContainerTest> {
    private final List<String> EXCLUSION_LIST = Arrays.asList(
                KafkaPropertiesAvroSerdeFactory.class)
            .stream()
            .map(Class::getName)
            .toList();
    
    protected KafkaContainerTestExcludeFilter(Class<KafkaContainerTest> testClass) {
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
