package org.dbg;

import java.util.Set;

import java.net.UnknownHostException;
import java.util.concurrent.ExecutionException;
import java.lang.InterruptedException;

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.commands.datatypes.FetchSet;
import com.basho.riak.client.api.commands.datatypes.SetUpdate;
import com.basho.riak.client.api.commands.datatypes.UpdateSet;
import com.basho.riak.client.api.commands.datatypes.Context;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.query.crdt.types.RiakSet;
import com.basho.riak.client.core.util.BinaryValue;

/**
 * Hello world!
 *
 */
public class App {
    public static void main( String[] args )
        throws UnknownHostException, ExecutionException, InterruptedException {
        System.out.println("Initializing client ...\n");
        RiakClient client = RiakClient.newClient("192.168.33.11, 192.168.33.12, 192.168.33.13, 192.168.33.14, 192.168.33.15");
        System.out.println("Client initialized!");

        System.out.println("Creating set ...");
        Location citiesSet = new Location(new Namespace("sets", "travel"), "cities");
        System.out.println("Set object created!");
        listSet(client, citiesSet);

        System.out.println("Adding to set ...");
        addToSet(client, citiesSet, "Toronto");
        listSet(client, citiesSet);
        addToSet(client, citiesSet, "Montreal");
        listSet(client, citiesSet);

        System.out.println("Removing from set ...");
        removeFromSet(client, citiesSet, "Toronto");
        listSet(client, citiesSet);

        enumerateSet(client, citiesSet);

        System.exit(0);
    }

    private static void listSet(RiakClient client, Location inputSet)
        throws ExecutionException, InterruptedException {
        FetchSet fetch              = new FetchSet.Builder(inputSet).build();
        FetchSet.Response response  = client.execute(fetch);
        RiakSet set                 = response.getDatatype();
        System.out.println("Set:\n" + set + "\n" + set.view() + "\n");
    }

    private static void addToSet(RiakClient client, Location set, String item)
        throws ExecutionException, InterruptedException {
        SetUpdate su        = new SetUpdate().add(item);
        UpdateSet update    = new UpdateSet.Builder(set, su).build();
        client.execute(update);
        System.out.println(item + " added to Set: " + set + "\n");
    }

    private static void removeFromSet(RiakClient client, Location set, String item)
        throws ExecutionException, InterruptedException {
            FetchSet fetch              = new FetchSet.Builder(set).build();
            FetchSet.Response response  = client.execute(fetch);
            Context ctx                 = response.getContext();
            SetUpdate su                = new SetUpdate().remove(item);
            UpdateSet update            = new UpdateSet.Builder(set, su).withContext(ctx).build();
            client.execute(update);
            System.out.println(item + " removed from Set: " + set + "\n");
    }

    private static void enumerateSet(RiakClient client, Location set)
        throws ExecutionException, InterruptedException {
        FetchSet fetch              = new FetchSet.Builder(set).build();
        FetchSet.Response response  = client.execute(fetch);
        Set<BinaryValue> binarySet  = response.getDatatype().view();
        
        System.out.println("Cities:");
        for (BinaryValue city : binarySet) {
            System.out.println(" - " + city.toStringUtf8());
        }
    }
}
