package it.unipi.hadoop;

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.util.hash.MurmurHash;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

public class RatingBloomFilter {

    // fields
    boolean[] filter;
    int[] seeds;
    MurmurHash hashFunction;

    // constructors
    public RatingBloomFilter(int filterSize, int[] seeds) {
        this.filter = new boolean[filterSize];
        this.seeds = seeds;
        this.hashFunction = new MurmurHash();
    }

    public RatingBloomFilter(boolean[] filters, int[] seeds) {
        this.filter = filters;
        this.seeds = seeds;
    }

    // methods
    public void fillUp(ArrayList<Movie> movies) {
        for (Movie movie : movies) {
            fillUp(movie);
        }
    }

    public void fillUp(Movie movie) {
        byte[] valueToHash;
        int pos;
        valueToHash = movie.id.getBytes();
        for (int seed : seeds) {
            // run hash function to coimpute position
            pos = Math.abs(hashFunction.hash(valueToHash, valueToHash.length, seed)) % filter.length;
            filter[pos] = true;
        }
    }

    public boolean lookUp(Movie movie) {
        boolean result = false;
        if (filter != null) {
            byte[] valueToHash = movie.id.getBytes();
            result = true;
            int pos;
            for (int seed : seeds) {
                // run hash function to compute the position
                pos = Math.abs(hashFunction.hash(valueToHash, valueToHash.length, seed)) % filter.length;
                result = result && filter[pos];
            }
        }
        return result;
    }

    public void saveToFile() {
        // saves the filters to txt file
    }

    public String getSeedsAsString() {
        String result = "";
        ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
        try {
            result = ow.writeValueAsString(this.seeds);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return result;
    }

    public String getFilterAsString() {
        String result = "";
        ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
        try {
            result = ow.writeValueAsString(this.filter);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return result;
    }

    public String toString() {
        return getSeedsAsString() + "\n" + getFilterAsString();
    }

    // static methods
    public static boolean[][] mergeFilters(List<boolean[][]> listOfFilters) {
        boolean[][] result = null;
        if (listOfFilters.size() > 0) {
            result = listOfFilters.remove(0);
            for (boolean[][] filterSet : listOfFilters) {
                for (int i = 0; i < result.length; i++) {
                    for (int j = 0; j < result[0].length; i++) {
                        result[i][j] = result[i][j] || filterSet[i][j];
                    }
                }
            }
        }
        return result;
    }
}
