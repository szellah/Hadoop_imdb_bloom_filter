package it.unipi.hadoop;

import java.nio.ByteBuffer;
import java.util.List;
import org.apache.hadoop.util.hash.MurmurHash;

public class RatingBloomFilter {

    //fields
    boolean[][] filters;
    int[] seeds;
    MurmurHash hashFunction;

    //constructors
    public RatingBloomFilter(int filtersSize, int[] seeds){
        this.filters = new boolean[10][filtersSize];
        this.seeds = seeds;
        this.hashFunction = new MurmurHash();
    }

    public RatingBloomFilter(boolean[][] filters, int[] seeds){
        this.filters = filters;
        this.seeds = seeds;
    }
    
    //methods
    public void fillUp(float[] ratings){
        int n;
        boolean[] filter;
        byte[] valueToHash;
        int pos;
        for (float rating : ratings) {
            n = Math.round(rating);
            valueToHash = ByteBuffer.allocate(4).putFloat(rating).array();
            filter = filters[n-1];
            for (int seed : seeds) {
                //run hash function to coimpute position
                pos = hashFunction.hash(valueToHash, valueToHash.length, seed);
                filter[pos] = true;
            }
        }
    }

    public boolean lookUp(float rating){
        int n = Math.round(rating);
        boolean[] filter = filters[n-1];
        byte[] valueToHash = ByteBuffer.allocate(4).putFloat(rating).array();
        boolean result = true;
        int pos;
        for (int seed : seeds) {
            //run hash function to compute the position
            pos = hashFunction.hash(valueToHash, valueToHash.length, seed);
            result = result && filter[pos];
        }
        return result;
    }

    public void saveToFile(){
        //saves the filters to txt file
    }

    //static methods
    public static boolean[][] mergeFilters(List<boolean[][]> listOfFilters){
        boolean[][] result = listOfFilters.remove(0);
        for (boolean[][] filterSet : listOfFilters) {
            for (int i = 0; i < result.length; i++) {
                for (int j = 0; j < result[0].length; i++) {
                    result[i][j] = result[i][j] || filterSet[i][j];
                } 
            }
        }
        return result;
    }
}
