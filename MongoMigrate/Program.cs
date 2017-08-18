using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MongoMigrate
{
    class Program
    {
        static void Main(string[] args)
        {
            MainAsync(args).Wait();
            Console.WriteLine("Press Enter...");
            Console.ReadLine();
        }

        static async Task MainAsync(string[] args)
        {
            
        }
    }
}
