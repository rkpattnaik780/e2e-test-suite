package io.managed.services.test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.managed.services.test.framework.TestListener;
import io.vertx.core.json.jackson.DatabindCodec;
import lombok.extern.log4j.Log4j2;
import org.testng.annotations.Listeners;

@Log4j2
@Listeners({TestListener.class})
public abstract class TestBase {

    static {
        //  object mappers necessary for correct encode as JSON (global setting)
        ObjectMapper mapper = DatabindCodec.mapper();
        mapper.registerModule(new JavaTimeModule());
        ObjectMapper prettyMapper = DatabindCodec.prettyMapper();
        prettyMapper.registerModule(new JavaTimeModule());

        log.info("### Environment variables:");
        Environment.getValues().forEach((key, value) -> log.info("{}: {}", key, value));

        log.info("### System properties:");
        System.getProperties().forEach((key, value) -> log.info("{}: {}", key, value));
    }
}
