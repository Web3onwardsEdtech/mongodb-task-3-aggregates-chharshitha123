const { MongoClient } = require('mongodb');

async function insertAndMatchMovies() {
  const uri = 'mongodb://localhost:27017';
  const client = new MongoClient(uri, { useNewUrlParser: true, useUnifiedTopology: true });

  try {
    await client.connect();
    const database = client.db('MoviesDB');
    const collection = database.collection('Movies');

    
    const movies = [
      { title: 'Inception', genre: 'Sci-Fi', popularity: 91 },
      { title: 'Interstellar', genre: 'Sci-Fi', popularity: 85 },
      { title: 'The Dark Knight', genre: 'Action', popularity: 95 },
      { title: 'Avengers', genre: 'Action', popularity: 92 },
      { title: 'Joker', genre: 'Drama', popularity: 75 },
      { title: 'Parasite', genre: 'Drama', popularity: 98 },
      { title: 'Frozen', genre: 'Animation', popularity: 60 },
      { title: 'Toy Story', genre: 'Animation', popularity: 65 },
      { title: 'The Room', genre: 'Drama', popularity: 40 },
      { title: 'Cats', genre: 'Musical', popularity: 30 }
    ];

   
    const insertResult = await collection.insertMany(movies);
    console.log(`${insertResult.insertedCount} movies inserted.`);

    
    const matchResult = await collection.aggregate([
      { $match: { popularity: { $gt: 50 } } },
      { $group: {
          _id: null, 
          matchedMovies: { $push: "$$ROOT" }, 
          count: { $sum: 1 } 
      }}
    ]).toArray();

    
    if (matchResult.length > 0) {
      console.log('Matched Movies:', matchResult[0].matchedMovies);
      console.log('Count of Matched Movies:', matchResult[0].count);
    } else {
      console.log('No matched movies found.');
    }

    
    const groupResult = await collection.aggregate([
      { $match: { popularity: { $gt: 50 } } },
      { $unwind: "$genre" },
      { 
        $group: { 
          _id: "$genre", 
          averagePopularity: { $avg: "$popularity" },
          movieCount: { $sum: 1 } 
        } 
      }
    ]).toArray();
    console.log('Grouped Movies with Counts:', groupResult);

   
    const sortedResult = await collection.aggregate([
      { $match: { popularity: { $gt: 50 } } },
      { $unwind: "$genre" },
      { 
        $group: { 
          _id: "$genre", 
          averagePopularity: { $avg: "$popularity" },
          movieCount: { $sum: 1 } 
        } 
      },
      { $sort: { averagePopularity: -1 } }
    ]).toArray();
    console.log('Sorted Movies by Genre:', sortedResult);

  } finally {
    await client.close();
  }
}

insertAndMatchMovies().catch(console.dir);
