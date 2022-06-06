package it.unipi.hadoop;

import java.util.List;

public class RatingBloomFilter {

    //fields
    boolean[][] filters;
    int[] seeds;

    //constructors
    public RatingBloomFilter(int filtersSize, int[] seeds){
        this.filters = new boolean[10][filtersSize];
        this.seeds = seeds;
    }

    public RatingBloomFilter(boolean[][] filters, int[] seeds){
        this.filters = filters;
        this.seeds = seeds;
    }
    
    //methods
    public void fillUp(float[] ratings){
        int n;
        boolean[] filter;
        for (float rating : ratings) {
            n = Math.round(rating);
            filter = filters[n-1];
            for (int seed : seeds) {
                //run hash with certain seed;
            }
        }
    }

    public boolean lookUp(float rating){
        int n = Math.round(rating);
        boolean[] filter = filters[n-1];
        boolean result = true;
        for (int seed : seeds) {
            //run hash function to compute the position
            int pos = 0;
            result = result && filter[pos];
        }
        return result;
    }

    public void saveToFile(){
        //saves the filters to txt file
    }

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
