package org.neo4j.elasticsearch;

import org.junit.Test;
import org.neo4j.elasticsearch.config.ElasticSearchIndexSpec;

import java.text.ParseException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.neo4j.elasticsearch.config.ElasticSearchIndexSpecParser.parseIndexSpec;

public class ElasticSearchIndexSpecParserTest {

    @Test
    public void parsing_index_spec_should_work() throws ParseException {
        Map<String, List<ElasticSearchIndexSpec>> rv = parseIndexSpec("index_name:Label(foo,bar,quux),other_index_name:OtherLabel(baz,quuxor)");
        assertEquals(2, rv.size());
        assertEquals(new HashSet<>(asList("Label", "OtherLabel")), rv.keySet());
    }

    @Test
    public void parsing_bad_index_spec_should_return_nothing() throws ParseException {
        Map rv = parseIndexSpec("index_name:Label(foo,bar");
        assertEquals(0, rv.size());
        rv = parseIndexSpec("index_name:Label");
        assertEquals(0, rv.size());
        rv = parseIndexSpec("Label");
        assertEquals(0, rv.size());
    }

    @Test(expected = ParseException.class)
    public void parsing_index_spec_with_twice_indexname_should_fail() throws ParseException {
        Map rv = parseIndexSpec("index_name:Label(foo,bar),index_name:Label(quux)");
    }


}
