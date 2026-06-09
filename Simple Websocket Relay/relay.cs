using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Net;
using System.Net.WebSockets;
using System.Text;
using System.Threading.Tasks;
using static System.Runtime.InteropServices.JavaScript.JSType;

namespace Simple_Websocket_Relay
{
    public class relay
    {

        ConcurrentDictionary<int, Room> rooms = new ConcurrentDictionary<int, Room>();
        int nextID = 0;

        public async Task start()
        {
           await Main();
        }

        private async Task Main()
        {
            HttpListener listener = new HttpListener();
            listener.Prefixes.Add("http://localhost:8080/");
            listener.Start();

            Console.WriteLine("Server started");

            while (true)
            {
                HttpListenerContext context =
                    await listener.GetContextAsync();

                if (context.Request.IsWebSocketRequest)
                {
                    _ = HandleClient(context);
                }
                else
                {
                    context.Response.StatusCode = 400;
                    context.Response.Close();
                }
            }
        }

        private async Task HandleClient(HttpListenerContext context)
        {
            WebSocketContext wsContext =
                await context.AcceptWebSocketAsync(null);

            WebSocket socket = wsContext.WebSocket;

            byte[] buffer = new byte[1024];

            bool isSocketInGame = false;
            int clientID = GetID();
            Room? clientsroom = null;

            try
            {
                while (socket.State == WebSocketState.Open) 
                {
                    var result = await socket.ReceiveAsync(
                        new ArraySegment<byte>(buffer),
                        CancellationToken.None
                    );

                    if (result.MessageType == WebSocketMessageType.Close)
                    {
                        Console.WriteLine("Client disconnected");
                        await socket.CloseAsync(
                            WebSocketCloseStatus.NormalClosure,
                            "Bye",
                            CancellationToken.None
                        );
                        break;
                    }

                    if (!isSocketInGame)
                    {
                        string message = Encoding.UTF8.GetString(buffer, 0, result.Count);
                        string response = "";

                        if(message == "CreateRoom")
                        {
                            try
                            {
                                int id = CreateRoom();
                                var joinroomresult = JoinRoom(id, socket, clientID);
                                if (!joinroomresult.success) throw new Exception("join room failed");
                                clientsroom = joinroomresult.room;
                                response = $"createroom:ok:{id}";
                                isSocketInGame = true;
                            }
                            catch(Exception ex)
                            {
                                Console.WriteLine("some problem accoured : " + ex);
                                response = $"roomcreate:fail";
                            }
                        }
                        else if (message.StartsWith("JoinRoom:"))
                        {
                            try
                            {
                                int id = int.Parse(message.Split(":")[1]);
                                var joinroomresult = JoinRoom(id, socket, clientID);
                                if (!joinroomresult.success) throw new Exception("join room failed");
                                clientsroom = joinroomresult.room;
                                response = $"joinroom:ok";
                                isSocketInGame = true ;
                            }
                            catch
                            {
                                Console.WriteLine("Bad JoinRoom Message : " + message);
                                response = $"joinroom:fail";
                            }
                        }

                        if (!string.IsNullOrEmpty(response)) 
                        { 
                            byte[] responseBytes = Encoding.UTF8.GetBytes(response);
                            await socket.SendAsync(
                                new ArraySegment<byte>(responseBytes),
                                WebSocketMessageType.Text,
                                true,
                                CancellationToken.None
                            );
                        }
                    }
                    else 
                    {

                        foreach (KeyValuePair<int, WebSocket> player in clientsroom.players.ToArray())
                        {
                            if (player.Key == clientID) continue;

                            WebSocket playersSocket = player.Value;

                            if (playersSocket.State != WebSocketState.Open) continue;

                            try
                            {
                                await playersSocket.SendAsync(
                                     new ArraySegment<byte>(buffer, 0, result.Count),
                                     WebSocketMessageType.Binary,
                                     true,
                                     CancellationToken.None
                                );
                            }
                            catch
                            {
                                clientsroom.players.TryRemove(player.Key, out _);
                            }
                        }
                    }
                }

            }
            catch (WebSocketException exe)
            {
                Console.WriteLine("Client disconnected (socket error)");
            }
            catch (Exception ex) 
            { 
                Console.WriteLine(ex.ToString());
            }
            finally
            {
                if (clientsroom != null)
                {
                    clientsroom.players.TryRemove(clientID, out _);

                    if (clientsroom.players.IsEmpty)
                        rooms.TryRemove(clientsroom.roomID, out _);
                }

                socket.Dispose();
            }
            Console.WriteLine("Client connected!");

        }
        
        private int CreateRoom()
        {
            Room room = new Room();
            Random random = new Random();
            room.roomID = random.Next(1000, 10000);
            rooms.TryAdd(room.roomID, room);
            return room.roomID;
        }

        private (bool success, Room? room) JoinRoom(int RoomID,WebSocket client,int clientID)
        {
            if (rooms.TryGetValue(RoomID, out Room room)) 
            {
                room.players.TryAdd(clientID, client);
                return (true,room);
            }
            else
            {
                return (false,null);
            }

        }

        private int GetID()
        {
            return nextID++;
        }

    }
}

public class Room()
{
    public int roomID;
    public ConcurrentDictionary<int, WebSocket> players = new ConcurrentDictionary<int, WebSocket>();
}
