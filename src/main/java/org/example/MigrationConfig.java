package org.example;

import com.marklogic.client.ext.helper.DatabaseClientProvider;
import com.marklogic.client.ext.helper.LoggingObject;
import com.marklogic.spring.batch.item.rdbms.TableItemWithMetadataReader;
import com.marklogic.spring.batch.item.writer.TripleWriter;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.support.PassThroughItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.util.StringUtils;

import javax.sql.DataSource;
import java.util.Map;

/**
 * Spring Configuration object that defines the Spring Batch components we need - a Reader, an optional Processor, and
 * a Writer.
 */
@EnableBatchProcessing
@Import(value = {com.marklogic.spring.batch.config.MarkLogicBatchConfiguration.class})
@PropertySource("file:./job.properties")
public class MigrationConfig extends LoggingObject {

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

		ItemReader<Map<String, Object>> reader;
		if ("true".equals(allTables)) {
			logger.info("Migrate all tables: " + allTables);
			reader = new TableItemWithMetadataReader(this.dataSource(), "");
		} else if (StringUtils.hasText(tableName)) {
			logger.info("Table Name: " + tableName);
			reader = new TableItemWithMetadataReader(this.dataSource(), "", tableName);
		} else {
			logger.error("Either table_name is not empty or all_table is true.");
			throw new RuntimeException();
		}

		TripleWriter writer = new TripleWriter(databaseClientProvider.getDatabaseClient(), graphName, baseIri);

		// Run the job!
		logger.info("Initialized components, launching job");
		return stepBuilderFactory.get("step1")
			.<Map<String, Object>, Map<String, Object>>chunk(10000)
			.reader(reader)
			.processor(new PassThroughItemProcessor<>())
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
