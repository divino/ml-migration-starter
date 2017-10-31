package custom;

import com.hp.hpl.jena.datatypes.xsd.XSDDatatype;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.NodeFactory;
import com.hp.hpl.jena.graph.Triple;
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

	private String baseIri;

	public ColumnMapProcessor(String baseIri) {
		this.baseIri = baseIri;
	}

	@Override
	public List<Triple> process(Map<String, Object> item) throws Exception {
		String pk = (String) item.get(ColumnMapRowMapper.PK_MAP_KEY);
		String name = (String) item.get(ColumnMapRowMapper.NAME_MAP_KEY);
		Map<String, String> metadata = (Map<String, String>) item.get(ColumnMapRowMapper.METADATA_MAP_KEY);
		List<Triple> triples = new ArrayList<>();
		for (Map.Entry<String, Object> entry : item.entrySet()) {
			if (!entry.getKey().startsWith(ColumnMapRowMapper.PRIVATE_MAP_KEY_PREFIX)) {
				Node object = null;
				if (metadata.get(entry.getKey()).equals("VARCHAR")) {
					object = NodeFactory.createLiteral(entry.getValue().toString());
				} else if (metadata.get(entry.getKey()).equals("INTEGER")) {
					object = NodeFactory.createLiteral(entry.getValue().toString(), XSDDatatype.XSDinteger);
				} else if (metadata.get(entry.getKey()).equals("DATE")) {
					object = NodeFactory.createLiteral(entry.getValue().toString(), XSDDatatype.XSDdate);
				}
				triples.add(
					new Triple(
						NodeFactory.createURI(baseIri + "/" + name + "#" + item.get(pk)),
						NodeFactory.createURI(baseIri + "/" + name + "/" + entry.getKey()),
						object
					)
				);
			}
		}
		return triples;
	}
}
