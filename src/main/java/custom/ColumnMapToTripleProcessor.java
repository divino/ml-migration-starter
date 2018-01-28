package custom;

import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.springframework.batch.item.ItemProcessor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This is a very basic processor for taking a column map (a Map<String, Object>) and serializing it via a
 * ColumnMapSerializer, and then providing very basic support for setting permissions and collections.
 * marklogic-spring-batch provides other options for e.g. customizing the URI. Feel free to customize any way you'd like.
 */
public class ColumnMapToTripleProcessor implements ItemProcessor<Map<String, Object>, List<Triple>> {

	private String baseIri;

	public ColumnMapToTripleProcessor(String baseIri) {
		this.baseIri = baseIri;
	}

	@Override
	public List<Triple> process(Map<String, Object> item) throws Exception {
		Map<String, Object> metadata = (Map<String, Object>) item.get(MetadataReader.META_MAP_KEY);
		String tableName = (String) metadata.get(MetadataReader.TABLE_NAME_MAP_KEY);
		String pk = (String) metadata.get(MetadataReader.PK_MAP_KEY);
		Map<String, Object> retval = new HashMap<>();
		List<Triple> triples = new ArrayList<>();

		for (Map.Entry<String, Object> entry : item.entrySet()) {
			if (null != entry.getValue() &&
					null != entry.getKey() &&
					!entry.getKey().equals(MetadataReader.META_MAP_KEY)) {

				Node object = null;
				if ("INTEGER".equals(metadata.get(entry.getKey()))) {
					object = NodeFactory.createLiteral(entry.getValue().toString(), XSDDatatype.XSDinteger);
				} else if ("DATE".equals(metadata.get(entry.getKey()))) {
					object = NodeFactory.createLiteral(entry.getValue().toString(), XSDDatatype.XSDdate);
				} else if ("DECIMAL".equals(metadata.get(entry.getKey()))) {
					object = NodeFactory.createLiteral(entry.getValue().toString(), XSDDatatype.XSDdecimal);
				} else {
					object = NodeFactory.createLiteral(entry.getValue().toString());
				}

				triples.add(
					new Triple(
						NodeFactory.createURI(baseIri + "/" + tableName + "#" + item.get(pk)),
						NodeFactory.createURI(baseIri + "/" + tableName + "/" + entry.getKey()),
						object
					)
				);
			}
		}

		return triples;
	}
}
