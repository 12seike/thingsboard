package org.thingsboard.server.dao.timeseries.tdengine;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.thingsboard.server.dao.timeseries.TimeseriesDao;
import org.thingsboard.server.dao.timeseries.TimeseriesLatestDao;

@Configuration
@ConditionalOnProperty(prefix = "ts.kv", name = "type", havingValue = "TDENGINE")
public class TdEngineTimeseriesConfiguration {

    @Bean
    public TimeseriesDao timeseriesDao() {
        return new TdEngineTimeseriesDao();
    }

    @Bean
    public TimeseriesLatestDao timeseriesLatestDao() {
        return new TdEngineTimeseriesLatestDao();
    }
}