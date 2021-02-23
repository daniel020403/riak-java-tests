package org.dbg;

import java.util.Set;

import java.net.UnknownHostException;
import java.util.concurrent.ExecutionException;
import java.lang.InterruptedException;

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.commands.datatypes.FetchSet;
import com.basho.riak.client.api.commands.datatypes.GSetUpdate;
import com.basho.riak.client.api.commands.datatypes.UpdateSet;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.query.crdt.types.RiakSet;
import com.basho.riak.client.core.util.BinaryValue;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ) 
        throws UnknownHostException, ExecutionException, InterruptedException {
        System.out.println("Initializing client ...\n");
        RiakClient client = RiakClient.newClient("192.168.33.11, 192.168.33.12, 192.168.33.13, 192.168.33.14, 192.168.33.15");
        System.out.println("Client initialized!");

        System.out.println("Creating gset ...");
        Location citiesGSet = new Location(new Namespace("gsets", "travel"), "cities");
        System.out.println("Set object created!");
        listGSet(client, citiesGSet);

        System.out.println("Adding to set ...");
        addToGSet(client, citiesGSet, "Toronto");
        listGSet(client, citiesGSet);
        addToGSet(client, citiesGSet, "Montreal");
        listGSet(client, citiesGSet);

        enumerateGSet(client, citiesGSet);

        System.exit(0);
    }

    private static void listGSet(RiakClient client, Location inputGSet)
        throws ExecutionException, InterruptedException {
        FetchSet fetch              = new FetchSet.Builder(inputGSet).build();
        FetchSet.Response response  = client.execute(fetch);
        RiakSet set                 = response.getDatatype();
        System.out.println("Set:\n" + set + "\n" + set.view() + "\n");
    }

    private static void addToGSet(RiakClient client, Location gset, String item)
        throws ExecutionException, InterruptedException {
        GSetUpdate su        = new GSetUpdate().add(item);
        UpdateSet update    = new UpdateSet.Builder(gset, su).build();
        client.execute(update);
        System.out.println(item + " added to Set: " + gset + "\n");
    }

    private static void enumerateGSet(RiakClient client, Location gset)
        throws ExecutionException, InterruptedException {
        FetchSet fetch              = new FetchSet.Builder(gset).build();
        FetchSet.Response response  = client.execute(fetch);
        Set<BinaryValue> binarySet  = response.getDatatype().view();
        
        System.out.println("Cities:");
        for (BinaryValue city : binarySet) {
            System.out.println(" - " + city.toStringUtf8());
        }
    }
}
