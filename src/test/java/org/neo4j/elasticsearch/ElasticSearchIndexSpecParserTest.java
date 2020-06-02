package org.neo4j.elasticsearch;

import org.neo4j.elasticsearch.config.ElasticSearchIndexSpec;
import org.junit.Test;

import java.text.ParseException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static org.neo4j.elasticsearch.config.ElasticSearchIndexSpecParser.parseIndexSpec;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

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
        Map rv = parseIndexSpec("department:Department(fullname,name),dnsrr:DnsRr(rr_all_value,rr_full_name,value1),dnszone:DnsZone(dnszone_name),esx:Esx(esx_name,hostname),ins:Ins(ins,name),entity:Entity(entity_id),moa:Moa(direction),sponsorpayer:SponsorPayer(label,direction),location:Location(fullname,name),middleware:Middleware(software,version),person:Person(nni,firstname,lastname,email),server:Server(system_name,alias),serverinterface:ServerInterface(interface_id),macaddress:MacAddress(mac_address),ipaddress:IpAddress(ip_address),lpp:Lpp(name),businessprocess:BusinessProcess(name),servicechain:ServiceChain(name),operatingsystem:OperatingSystem(type,version,level),domain:Domain(domain_name),loadbalancer:Loadbalancer(name),lbvip:LbVip(ip_address),lbpool:LbPool(lb_name),middleware:Middleware(software),middlewareversion:MiddlewareVersion(version),ostype:OsType(type),oslevel:OsLevel(level),osversion:OsVersion(version)");
    }


}
