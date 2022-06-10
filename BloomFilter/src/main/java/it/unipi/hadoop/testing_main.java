package it.unipi.hadoop;



public class testing_main {
    public static void main(String[] args) {
        Movie[] movies = new Movie[]{
            new Movie("hej1", 3.4f),
            new Movie("hej2", 4.5f),
            new Movie("hej3", 7.8f),
        };
        Movie mockMovie = new Movie("siema",3.4f);
        RatingBloomFilterFactory rbff = new RatingBloomFilterFactory();
        RatingBloomFilter rbf = rbff.CreateBloomFilter();
        System.out.println(rbf.lookUp(movies[1]));
        System.out.println(rbf.lookUp(mockMovie));
        System.out.println(rbf.toString());
    }
}
