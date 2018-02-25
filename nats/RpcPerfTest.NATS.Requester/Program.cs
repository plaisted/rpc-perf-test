using NATS.Client;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace RpcPerfTest.NATS.Client
{
    class Program
    {
        public static volatile int ReadyCount = 0;
        public static void Main(string[] args)
        {
            //starting trigger for all workers
            ManualResetEvent starter = new ManualResetEvent(false);

            //get cmd line options naively
            int workers = args.Length > 0 ? int.Parse(args[0]) : 1;
            int attempts = args.Length > 1 ? int.Parse(args[1]) : 1000;

            var connection = new ConnectionFactory().CreateConnection();
            
            //simple task bag
            List<Task<decimal>> tasks = new List<Task<decimal>>();
            //queue on concurrent workers
            for (var i = 0; i < workers; i++)
            {
                tasks.Add(Task.Run(() => RunWorker(attempts, connection, starter)));
            }

            var sw = new Stopwatch();
            sw.Start();
            //wait for workers to finish warming
            while (ReadyCount < workers && sw.ElapsedMilliseconds < 15000)
            {
                Thread.Sleep(100);
            }
            //did not all warm
            if (ReadyCount < workers)
            {
                throw new ApplicationException("Workers did not start in timeout.");
            }
            //start worker requests
            starter.Set();

            //time overall execution, some overhead from tasks etc but with large number of requests this is negligible.
            sw.Restart();
            Task.WaitAll(tasks.ToArray());
            sw.Stop();

            //get stats
            var averagePerWorker = Decimal.Divide(tasks.Select(x => x.Result).Sum(), workers);
            var rpsTotal = Decimal.Divide(attempts * workers, sw.ElapsedMilliseconds) * 1000;
            Console.WriteLine($"Per request : {averagePerWorker} ms.");
            Console.WriteLine($"Total RPS: {rpsTotal}.");
            Console.ReadLine();
        }

        public static async Task<Decimal> RunWorker(int count, IConnection connection, ManualResetEvent starter)
        {
            try
            {
                int success = 0;
                int failure = 0;
                if (starter != null)
                {
                    // ensure connection warm and wait
                    Console.WriteLine("Client warming.");
                    await RunWorker(10, connection, null);
                    Console.WriteLine("Warming finished.");
                    // bump ready count
                    Interlocked.Increment(ref ReadyCount);
                    // wait for trigger
                    starter.WaitOne();
                }
                var sw = new Stopwatch();
                sw.Start();
                for (var i = 0; i < count; i++)
                {
                    try
                    {
                        // If timeout is omitted here this does not work!!
                        // Connections do not finish "warmimg"
                        // Odd this all requests finish without timeout (100%) success.
                        var tsk = await connection.RequestAsync("test", Encoding.UTF8.GetBytes("ping"), 100);
                        success++;
                    } catch (Exception)
                    {
                        failure++;
                    }
                    
                }
                sw.Stop();
                if (starter != null)
                {
                    // post results if not a warmup
                    var result = Decimal.Divide(sw.ElapsedMilliseconds, count);
                    Console.WriteLine("Single ave: " + result);
                    Console.WriteLine("Success: " + Decimal.Divide(success, count)*100 + "%");
                    return result;
                }
                else
                {
                    return 0;
                }
            } catch(Exception e)
            {
                Console.WriteLine(e.ToString());
                throw e;
            }
        }
    }
}
