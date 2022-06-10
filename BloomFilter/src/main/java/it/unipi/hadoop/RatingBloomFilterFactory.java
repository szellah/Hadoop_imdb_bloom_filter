package it.unipi.hadoop;

// import java.io.File;  // Import the File class
// import java.io.FileNotFoundException;  // Import this class to handle errors
// import java.util.Scanner; // Import the Scanner class to read text files
// import java.util.ArrayList;
import java.util.Random;  

//It uses map reducer
public class RatingBloomFilterFactory {

    public RatingBloomFilter CreateBloomFilter(){
        //load up config file
        Float p = Float.parseFloat(System.getProperty("P_CONFIG"));
        Integer n = Integer.parseInt(System.getProperty("N_CONFIG"));
        Integer filterSize = ComputeM(p, n);
        Integer amountOfSeeds = ComputeK(n, filterSize);
        int[] seeds = GenerateSeeds(amountOfSeeds);
        //get lines from imdb

        //MAPPER PART
        //Divide the data into 5 
        //each worker should use this thing
        // ComputeSetOfFilters(filterSize, seeds, null);

        // List<boolean[][]> setsOfFilters = new ArrayList<boolean[][]>();

        //REDUCER PART
        
        // boolean[][] filters = RatingBloomFilter.mergeFilters(setsOfFilters);

        RatingBloomFilter result = new RatingBloomFilter(filterSize, seeds);
        // result.saveToFile();
        return result;
    }
   
    // private ArrayList<String> ReadConfigFile() {
    //     ArrayList<String> result = new ArrayList<String>();
    //     try {
    //       File myObj = new File("config.txt");
    //       Scanner myReader = new Scanner(myObj);  
    //       while (myReader.hasNextLine()) {
    //         String data = myReader.nextLine(); 
    //         result.add(data);
    //       }
    //       myReader.close();
    //     } catch (FileNotFoundException e) {
    //       System.out.println("An error occurred.");
    //       e.printStackTrace();
    //     }
    //     return result;
    // }

    private int[] GenerateSeeds(int n){
        int[] result = new int[n];
        Random random = new Random();
        for (int i = 0; i < result.length; i++) {
            result[i] = random.nextInt();
        }
        return result;
    }

    private int ComputeM(float p, int n) {  
        return (int) ((-n * Math.log(p)) / Math.pow(Math.log(2),2));
    }

    private int ComputeK(int n, int m) {  
        return (int) ((m/n) * Math.log(2));
    }

    // private boolean[][] ComputeSetOfFilters(int filterSize, int[] seeds, Movie[] datasubset){
    //     RatingBloomFilter bf = new RatingBloomFilter(filterSize, seeds);
    //     bf.fillUp(datasubset);
    //     return bf.filters;
    // }
}