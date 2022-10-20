using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;
using MongoDB.Bson;

namespace BsonParse
{
    internal class Program
    {
        private static ConcurrentQueue<int> _elementIndexes;
        private static ConcurrentQueue<BsonDocument> _bsonDocuments;
        private static string _findPattern;

        [DllImport("msvcrt.dll")]
        static extern unsafe int memcmp(void* ptr1, void* ptr2, int count);
        
        [DllImport("msvcrt.dll",  SetLastError = false)]
        static extern unsafe int memcpy(void* ptr1, void* ptr2, int count);
        
        public static async Task Main(string[] args)
        {
            // inits
            _findPattern = "_id";
            _elementIndexes = new ConcurrentQueue<int>();
            
            var timer = new Stopwatch();
            timer.Start();
            var startOperation = timer.ElapsedMilliseconds;
            
            // load json
            var jsonResponse = await GetJsonString();
            Console.WriteLine($"FileContent: {jsonResponse.Length} >> LoadTime: {timer.ElapsedMilliseconds - startOperation}ms");
            startOperation = timer.ElapsedMilliseconds;
            
            // find patterns
            Parallel.For(1, jsonResponse.Length, x => FindArrayElements(x, jsonResponse));
            Console.WriteLine($"FindArrayElements[{_elementIndexes.Count}]: {timer.ElapsedMilliseconds - startOperation}ms");
            startOperation = timer.ElapsedMilliseconds;
            
            // parse bson
            _bsonDocuments = new ConcurrentQueue<BsonDocument>();
            var elementsArray = _elementIndexes.ToArray();
            Array.Sort(elementsArray);
            Parallel.For(1, elementsArray.Length, x => ParseBsonElements(x, elementsArray, jsonResponse));
            Console.WriteLine($"BuildBson: {timer.ElapsedMilliseconds - startOperation}ms");
            startOperation = timer.ElapsedMilliseconds;
            
            Console.WriteLine($"CompleteTotal: {timer.ElapsedMilliseconds}ms");

            var bsonArray = _bsonDocuments.ToArray();
            Console.WriteLine($"BsonDocument[999]: {bsonArray[999]}");
        }

        private static unsafe void ParseBsonElements(int index, IReadOnlyList<int> elements, string content)
        {
            var start = elements[index - 1] - 2;
            var end = elements[index] - 3;
            var length = end - start;
            
            // fast substring
            var stringBytes = new byte[length];
            var stringElement = Encoding.UTF8.GetString(stringBytes);
            fixed (char* elementPointer = stringElement)
            {
                fixed (char* contentPointer = content)
                {
                    memcpy(elementPointer, (byte*)contentPointer + start * 2, length * 2);
                }
            }

            _bsonDocuments.Enqueue(BsonDocument.Parse(stringElement));
        }

        private static unsafe void FindArrayElements(int index, string content)
        {
            fixed (char* patternPointer = _findPattern)
            {
                fixed (char* contentPointer = content)
                {
                    // char == 2 byte
                    var result = memcmp(patternPointer, (byte*)contentPointer + index * 2, _findPattern.Length);
                    if (result == 0)
                    {
                        _elementIndexes.Enqueue(index);
                    }
                }
            }
        }

        private static async Task<string> GetJsonString()
        {
            var request = WebRequest.Create("https://github.com/igor-karpushin/MongoBsonTest/raw/main/BsonData.json");
            var response = await request.GetResponseAsync();

            var result = string.Empty;
            
            using (var stream = response.GetResponseStream())
            {
                if (stream == null) return result;
                using (var reader = new StreamReader(stream))
                {
                    result = await reader.ReadToEndAsync();
                }
            }

            return result;
        }
    }
}