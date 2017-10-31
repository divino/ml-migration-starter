package org.example;

import com.hp.hpl.jena.graph.Triple;
import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.helper.LoggingObject;
import com.marklogic.spring.batch.Options;
import com.marklogic.spring.batch.config.support.OptionParserConfigurer;
import custom.*;
import joptsimple.OptionParser;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemReader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.EnvironmentAware;
import org.springframework.context.annotation.Bean;
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
public class MigrationConfig extends LoggingObject implements EnvironmentAware, OptionParserConfigurer {

	private Environment env;

	/**
	 * By implementing this method in OptionParserConfigurer, a client can run the MigrationMain program and ask for
	 * help and see all of our custom command line options.
	 *
	 * @param parser
	 */
	@Override
	public void configureOptionParser(OptionParser parser) {
		parser.accepts("hosts", "Comma-delimited sequence of host names of MarkLogic nodes to write documents to").withRequiredArg();
		parser.accepts("sql", "The SQL query for selecting rows to migrate").withRequiredArg();
	}

	/**
	 * Defines the Spring Batch job. All we need here is to give it a name.
	 *
	 * @param jobBuilderFactory
	 * @param step
	 * @return
	 */
	@Bean
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
	                 @Value("#{jobParameters['hosts']}") String hosts,
	                 @Value("#{jobParameters['sql']}") String sql,
					 @Value("#{jobParameters['pk']}") String pk,
					 @Value("#{jobParameters['all_tables']}") String allTables,
					 @Value("#{jobParameters['graph_name']}") String graphName,
					 @Value("#{jobParameters['base_iri']}") String baseIri) {

		// Determine the Spring Batch chunk size
		int chunkSize = 100;
		String prop = env.getProperty(Options.CHUNK_SIZE);
		if (prop != null) {
			chunkSize = Integer.parseInt(prop);
		}

		logger.info("Chunk size: " + env.getProperty(Options.CHUNK_SIZE));
		logger.info("Hosts: " + hosts);
		if (StringUtils.hasText(sql) &&
			StringUtils.hasText(pk) &&
			StringUtils.hasText(graphName)) {
			logger.info("SQL: " + sql);
			logger.info("Primary Key: " + pk);
			logger.info("Graph Name: " + graphName);
		} else {
			//logger.info("Migrate all tables: " + allTables);
			logger.error("All tables not yet supported.");
			throw new RuntimeException();
		}

		ItemReader<Map<String, Object>> reader = null;
		if ("true".equals(allTables)) {
			// Use AllTablesReader to process rows from every table
			reader = new AllTablesItemReader(buildDataSource());
		} else {
			// Uses Spring Batch's JdbcCursorItemReader and Spring JDBC's ColumnMapRowMapper to map each row
			// to a Map<String, Object>. Normally, if you want more control, standard practice is to bind column values to
			// a POJO and perform any validation/transformation/etc you need to on that object.
			JdbcCursorItemReader<Map<String, Object>> r = new JdbcCursorItemReader();
			r.setRowMapper(new ColumnMapRowMapper(graphName, pk));
			r.setDataSource(buildDataSource());
			r.setSql(sql);
			r.setPrimaryKey(pk);
			r.setName(graphName);
			reader = r;
		}

		ColumnMapProcessor processor = new ColumnMapProcessor(baseIri);

		//TODO: find a way to pass the table name as graphName when processing all_tables = true
		RdfTripleItemWriter writer = new RdfTripleItemWriter(buildDatabaseClient(hosts), graphName);

		// Run the job!
		logger.info("Initialized components, launching job");
		return stepBuilderFactory.get("step1")
			.<Map<String, Object>, List<Triple>>chunk(chunkSize)
			.reader(reader)
			.processor(processor)
			.writer(writer)
			.build();
	}

	protected DatabaseClient buildDatabaseClient(String hosts) {
		Integer port = Integer.parseInt(env.getProperty(Options.PORT));
		String username = env.getProperty(Options.USERNAME);
		String password = env.getProperty(Options.PASSWORD);
		String database = env.getProperty(Options.DATABASE);
		String auth = env.getProperty(Options.AUTHENTICATION);
		DatabaseClientFactory.Authentication authentication = DatabaseClientFactory.Authentication.DIGEST;
		if (auth != null) {
			authentication = DatabaseClientFactory.Authentication.valueOf(auth.toUpperCase());
		}
		logger.info("Client username: " + username);
		logger.info("Client database: " + database);
		logger.info("Client authentication: " + authentication.name());
		String host = env.getProperty(Options.HOST);
		logger.info("Creating client for host: " + host);
		return DatabaseClientFactory.newClient(host, port, database, username, password, authentication);
	}

	/**
	 * Uses the very simple Spring JDBC DriverManagerDataSource to build a DataSource for our Reader to use. Since we
	 * by default only make a single JDBC query in this migration, we don't need any connection pooling support. But
	 * this is easily added via the many DataSource implementations that Spring JDBC providers.
	 * <p>
	 * Note that we're able to pull connection properties directly from the Spring Environment here.
	 *
	 * @return
	 */
	protected DataSource buildDataSource() {
		DriverManagerDataSource ds = new DriverManagerDataSource();
		ds.setDriverClassName(env.getProperty(Options.JDBC_DRIVER));
		ds.setUrl(env.getProperty(Options.JDBC_URL));
		ds.setUsername(env.getProperty(Options.JDBC_USERNAME));
		ds.setPassword(env.getProperty(Options.JDBC_PASSWORD));
		return ds;
	}

	@Override
	public void setEnvironment(Environment environment) {
		this.env = environment;
	}

}
