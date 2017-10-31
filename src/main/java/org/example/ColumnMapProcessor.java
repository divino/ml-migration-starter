package org.example;

import com.github.jsonldjava.core.RDFDatasetUtils;
import com.hp.hpl.jena.datatypes.RDFDatatype;
import com.hp.hpl.jena.graph.NodeFactory;
import com.hp.hpl.jena.graph.Triple;
import custom.ColumnMapRowMapper;
import org.springframework.batch.item.ItemProcessor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * This is a very basic processor for taking a column map (a Map<String, Object>) and serializing it via a
 * ColumnMapSerializer, and then providing very basic support for setting permissions and collections.
 * marklogic-spring-batch provides other options for e.g. customizing the URI. Feel free to customize any way you'd like.
 */
public class ColumnMapProcessor implements ItemProcessor<Map<String, Object>, List<Triple>> {

	private String uriPrefix = "http://localhost";

	public ColumnMapProcessor() {
	}

	@Override
	public List<Triple> process(Map<String, Object> item) throws Exception {
		String pk = (String) item.get(ColumnMapRowMapper.PK_MAP_KEY);
		String name = (String) item.get(ColumnMapRowMapper.NAME_MAP_KEY);
		Map<String, String> metadata = (Map<String, String>) item.get(ColumnMapRowMapper.METADATA_MAP_KEY);

		List<Triple> triples = new ArrayList<>();
		for (Map.Entry<String, Object> entry : item.entrySet()) {
			if (!entry.getKey().startsWith(ColumnMapRowMapper.PRIVATE_MAP_KEY_PREFIX)) {
				triples.add(
					new Triple(
						NodeFactory.createURI(uriPrefix + "/" + name + "#" + item.get(pk)),
						NodeFactory.createURI(entry.getKey()),
						NodeFactory.createLiteral(entry.getValue().toString())
					)
				);
			}
		}
		return triples;
	}
}
