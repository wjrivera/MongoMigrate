using System;
using System.Linq;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Bson.Serialization.Attributes;

namespace MongoMigrate
{
    public class Program
    {
        public static void Main()
        {
            DeleteLowestScore().GetAwaiter().GetResult();
            Console.WriteLine("Press Enter...");
            Console.ReadLine();
        }

        static async Task DeleteLowestScore()
        {
            var client = new MongoClient();
            var db = client.GetDatabase("test");
            var col = db.GetCollection<BsonDocument>("grades");

            var doc = await col.Find(new BsonDocument()).ToListAsync();

            var counter = 0;

            //Find all students and group them by student_id
            foreach (var studentId in doc.GroupBy(x => x["student_id"]))
            {
                counter++;
                Console.WriteLine(counter);
                
                // Iterate through all of the "homework" scores for this student
                var filterBuilder = Builders<BsonDocument>.Filter;
                var homeworkFilter = filterBuilder.Eq("student_id", studentId.Key) & filterBuilder.Eq("type", "homework");
                var homeworkScores = await col.Find(homeworkFilter).ToListAsync();

                // Identify the lowest score
                BsonDocument lowestScore = null;
                foreach (var score in homeworkScores)
                {
                    if (lowestScore == null || score["score"] < lowestScore["score"])
                    {
                        lowestScore = score;
                    }
                }

                // Remove that score from the collection
                if (lowestScore != null)
                {
                    await col.DeleteOneAsync(x => x["_id"] == lowestScore["_id"]);
                }

            }
        }

        static async Task BulkWriteMainTask()
        {
            var client = new MongoClient();
            var db = client.GetDatabase("test");
            var col = db.GetCollection<Widget>("widgets");
            await db.DropCollectionAsync("widgets");

            var docs = Enumerable.Range(0, 10).Select(i => new Widget { Id = i, X = i });
            await col.InsertManyAsync(docs);

            Console.WriteLine("BEFORE");
            await col.Find(new BsonDocument())
                .ForEachAsync(x => Console.WriteLine(x));

            var result = col.BulkWriteAsync(new WriteModel<Widget>[]
            {
                new DeleteOneModel<Widget>("{x: 5}"),
                new DeleteOneModel<Widget>("{x: 7}"),
                new UpdateManyModel<Widget>("{x: {$lt: 7}}", "{$inc: {x: 1}}")
            });



            Console.WriteLine("RESULTS");
            Console.WriteLine(result);

            Console.WriteLine("AFTER");
            await col.Find(new BsonDocument())
                .ForEachAsync(x => Console.WriteLine(x));
        }

        static async Task FindOneAndMainTask()
        {
            var client = new MongoClient();
            var db = client.GetDatabase("test");
            var col = db.GetCollection<Widget>("widgets");
            await db.DropCollectionAsync("widgets");

            var docs = Enumerable.Range(0, 10).Select(i => new Widget { Id = i, X = i });
            await col.InsertManyAsync(docs);

            Console.WriteLine("BEFORE");
            await col.Find(new BsonDocument())
                .ForEachAsync(x => Console.WriteLine(x));

            // Update
            var result = await col.FindOneAndUpdateAsync(ñ => ñ.X > 5, Builders<Widget>.Update.Inc(ñ => ñ.X, 1));
            
            // Delete
            var result3 = await col.FindOneAndDeleteAsync(ñ => ñ.X == 9);

            Console.WriteLine("RESULTS");
            Console.WriteLine(result);

            Console.WriteLine("AFTER");
            await col.Find(new BsonDocument())
                .ForEachAsync(x => Console.WriteLine(x));
        }

        static async Task DeleteManyMainTask()
        {
            var client = new MongoClient();
            var db = client.GetDatabase("test");
            var col = db.GetCollection<Widget>("widgets");
            await db.DropCollectionAsync("widgets");

            var docs = Enumerable.Range(0, 10).Select(i => new Widget { Id = i, X = i });
            await col.InsertManyAsync(docs);

            Console.WriteLine("BEFORE");
            await col.Find(new BsonDocument())
                .ForEachAsync(x => Console.WriteLine(x));

            var result = await col.DeleteManyAsync(ñ => ñ.X > 5);

            Console.WriteLine("AFTER");
            await col.Find(new BsonDocument())
                .ForEachAsync(x => Console.WriteLine(x));
        }

        static async Task UpdateManyMainTask()
        {
            var client = new MongoClient();
            var db = client.GetDatabase("test");
            var col = db.GetCollection<Widget>("widgets");
            await db.DropCollectionAsync("widgets");

            var docs = Enumerable.Range(0, 10).Select(i => new Widget {Id = i, X = i });
            await col.InsertManyAsync(docs);

            var result = await col.UpdateManyAsync(
                Builders<Widget>.Filter.Gt("x", 5), 
                Builders<Widget>.Update.Inc("x", 10).Set("J", 20));

            await col.Find(new BsonDocument())
                .ForEachAsync(x => Console.WriteLine(x));
        }

        static async Task FindSkipSortMainTask()
        {
            // Query lives on memory, pull everything
            var client = new MongoClient();
            var db = client.GetDatabase("test");
            var col = db.GetCollection<Person>("people");

            var list = await col.Find(new BsonDocument())
                //.Sort("{Age: 1}")
                .Sort(Builders<Person>.Sort.Ascending("Age"))
                //.Project("{Name: 1, Age: 1, _id:0}")
                //.Project(Builders<Person>.Projection.Include(ñ => ñ.Name).Include(ñ => ñ.Age).Exclude("_id"))
                .Project(ñ => new { ñ.Name, CalcAge = ñ.Age + 20 })
                .Skip(2)
                .Limit(5)
                .ToListAsync();

            foreach (var doc in list)
            {
                Console.WriteLine(doc);
            }
        }

        private static async Task FindOneMainAsync()
        {
            var client = new MongoClient();
            var db = client.GetDatabase("test");
            var col = db.GetCollection<BsonDocument>("people");

            // foreach method
            Console.WriteLine("FOR EACH METHOD");
            var filter1 = new BsonDocument("Name", "Rivera");        // Create filter for Name = "Rivera
            await col.Find(new BsonDocument(filter1)).ForEachAsync(doc => Console.WriteLine(doc));     // Print line per documents found, don't keep them around

            // Query lives on memory, pull everything
            Console.WriteLine("TO LIST METHOD");
            var filter2 = new BsonDocument("$and", new BsonArray
            {
                new BsonDocument("Age", new BsonDocument("$gt", 21)),
                new BsonDocument("Name", "Ricardo")
            });
            var list = await col.Find(filter2).ToListAsync();        // Find all documents, return all documents and put them memory
            foreach (var doc in list)
            {
                Console.WriteLine(doc);
            }

            // Query lives on memory, pull everything, use builder
            //Console.WriteLine("TO LIST METHOD USE BUILDER");
            //var client = new MongoClient();
            //var db = client.GetDatabase("test");
            //var col = db.GetCollection<Person>("people");


            //var builder = Builders<Person>.Filter;        //Builder template for creating filters
            //var filter = builder.Lt("Age", 30);
            //var list = await col.Find(filter)
            //    .ToListAsync();        // Find all documents, return all documents and put them memory
            //foreach (var doc in list)
            //{
            //    Console.WriteLine(doc);
            //}

            //Extremely granular and flexible, uses a cursor
            Console.WriteLine("TO CURSOR METHOD");
            using (var cursor = await col.Find(new BsonDocument()).ToCursorAsync())     // Find all documents (no filters), return cursor
            {
                while (await cursor.MoveNextAsync())        // Get one batch at a time
                {
                    foreach (var doc in cursor.Current)
                    {
                        Console.WriteLine(doc);
                    }
                }
            }
        }

        private static async Task InsertManyMainAsync(string[] args)
        {
            //var settings = new MongoClientSettings();
            var client = new MongoClient();
            var db = client.GetDatabase("test");
            var col = db.GetCollection<Person>("people");

            var doc1 = new Person
            {
                Name = "Rivera",
                Age = 7,
                Profession = "Student"
            };

            var doc2 = new Person
            {
                Name = "Martinez",
                Age = 26,
                Profession = "Cashier"
            };

            var doc3 = new Person
            {
                Name = "Luna",
                Age = 38,
                Profession = "Plumber"
            };

            Console.WriteLine(doc1.Id);

            await col.InsertManyAsync(new [] { doc1, doc2, doc3 });

            Console.WriteLine(doc1.Id);

        }

        private static async Task InsertOneMainAsync(string[] args)
        {
            //var settings = new MongoClientSettings();
            var client = new MongoClient();
            var db = client.GetDatabase("test");
            var col = db.GetCollection<Person>("people");

            var doc = new Person
            {
                Name = "Feng",
                Age = 23,
                Profession = "Developer"
            };

            Console.WriteLine(doc.Id);

            await col.InsertOneAsync(doc);

            Console.WriteLine(doc.Id);

        }

        class Person
        {
            public ObjectId Id { get; set; }
            public string Name { get; set; }
            public int Age { get; set; }
            public string Profession { get; set; }
        }

        [BsonIgnoreExtraElements]
        public class Widget
        {
            public int Id { get; set; }

            [BsonElement("x")]
            public int X { get; set; }

            public override string ToString()
            {
                return string.Format("Id: {0}, x: {1}", Id, X);
            }

        }
    }
}
