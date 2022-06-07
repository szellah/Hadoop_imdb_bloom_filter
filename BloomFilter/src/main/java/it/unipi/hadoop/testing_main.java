package it.unipi.hadoop;



public class testing_main {
    public static void main(String[] args) {
        RatingBloomFilter rbf = new RatingBloomFilter(5, new int[]{2,3,4});
        Movie[] movies = new Movie[]{
            new Movie("hej1", 3.4f),
            new Movie("hej2", 4.5f),
            new Movie("hej3", 7.8f),
        };
        Movie mockMovie = new Movie("siema",3.4f);
        rbf.fillUp(movies);
        System.out.println(rbf.lookUp(movies[1]));
        System.out.println(rbf.lookUp(mockMovie));
    }
}
