using NATS.Client;
using System;
using System.Text;

namespace RpcPerfTest.NATS.Responder
{
    class Program
    {
        static void Main(string[] args)
        {
            using (var client = new ProxyClient())
            using (var sub = client.Start())
            {
                Console.WriteLine("Responder started.");
                Console.ReadLine();
            }
        }
    }

    public class ProxyClient : IDisposable
    {
        private IConnection connection;

        public ProxyClient()
        {
            connection = new ConnectionFactory().CreateConnection();
        }

        private void Process(object _, MsgHandlerEventArgs args)
        {
            var msg = new Msg();
            msg.Subject = args.Message.Reply;
            msg.Data = Encoding.UTF8.GetBytes("pong");
            connection.Publish(msg);
            connection.Flush();
        }

        public IAsyncSubscription Start()
        {
            return connection.SubscribeAsync("test", "test", Process);
        }

        public void Dispose()
        {
            connection.Dispose();
        }
    }
}
