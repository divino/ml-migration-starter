package org.example;

import com.marklogic.client.ext.helper.DatabaseClientProvider;
import custom.*;
import org.apache.jena.graph.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.util.StringUtils;

import javax.sql.DataSource;
import java.util.List;
import java.util.Map;

/**
 * Spring Configuration object that defines the Spring Batch components we need - a Reader, an optional Processor, and
 * a Writer.
 */
@EnableBatchProcessing
@Import(value = {com.marklogic.spring.batch.config.MarkLogicBatchConfiguration.class})
@PropertySource("file:./job.properties")
public class MigrationConfig {

	protected final Logger logger = LoggerFactory.getLogger(getClass());

	@Autowired
	private Environment env;

	private final String JOB_NAME = "migrationJob";

	/**
	 * Defines the Spring Batch job. All we need here is to give it a name.
	 *
	 * @param jobBuilderFactory
	 * @param step
	 * @return
	 */
	@Bean(name = JOB_NAME)
	public Job job(JobBuilderFactory jobBuilderFactory, Step step) {
		return jobBuilderFactory.get("migrationJob").start(step).build();
	}

	/**
	 * Defines the single step in our Spring Batch job. A key feature provided by marklogic-spring-batch is that
	 * command-line arguments can be referenced via the Value annotations shown below.
	 *
	 * @return
	 */
	@Bean
	@JobScope
	public Step step(StepBuilderFactory stepBuilderFactory,
					 DatabaseClientProvider databaseClientProvider,
					 @Value("#{jobParameters['graph_name']}") String graphName,
	                 @Value("#{jobParameters['table_name']}") String tableName,
					 @Value("#{jobParameters['all_tables']}") String allTables,
					 @Value("#{jobParameters['base_iri']}") String baseIri) {

		ItemReader<Map<String, Object>> reader = null;
		if ("true".equals(allTables)) {
			logger.info("Migrate all tables: " + allTables);
			reader = new TableItemReaderWithMetadata(this.dataSource());
		} else if (StringUtils.hasText(tableName)) {
			logger.info("Table Name: " + tableName);
			reader = new TableItemReaderWithMetadata(this.dataSource(), "", tableName);
		} else {
			logger.error("Either table_name is not empty or all_table is true.");
			throw new RuntimeException();
		}

		ColumnMapToTripleProcessor processor = new ColumnMapToTripleProcessor(baseIri);

		TripleWriter writer = new TripleWriter(databaseClientProvider.getDatabaseClient(), graphName);

		// Run the job!
		logger.info("Initialized components, launching job");
		return stepBuilderFactory.get("step1")
			.<Map<String, Object>, List<Triple>>chunk(1000)
			.reader(reader)
			.processor(processor)
			.writer(writer)
			.build();
	}

	@Bean
	public DataSource dataSource() {
		DriverManagerDataSource dataSource = new DriverManagerDataSource();
		dataSource.setDriverClassName(env.getRequiredProperty("jdbc_driver"));
		dataSource.setUrl(env.getRequiredProperty("jdbc_url"));
		dataSource.setUsername(env.getRequiredProperty("jdbc_username"));
		dataSource.setPassword(env.getRequiredProperty("jdbc_password"));
		return dataSource;
	}

}
