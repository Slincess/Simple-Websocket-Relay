using Simple_Websocket_Relay;
using System;

namespace MyApp
{
    internal class Program
    {
        static async Task Main(string[] args)
        {
            var relay = new relay();
            await relay.start();
        }
    }
}