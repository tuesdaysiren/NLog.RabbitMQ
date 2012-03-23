using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;

namespace DemoListener
{
	internal class Program
	{
		private static object _locker = new object();

		private static void Main(string[] args)
		{
			var factory = new ConnectionFactory
			{
				HostName = "localhost",
				UserName = "guest",
				Password = "guest",
				Protocol = Protocols.DefaultProtocol
			};

			while (true)
			{
				try
				{
					while (true)
					{
						try
						{
							using (var c = factory.CreateConnection())
							using (var m = c.CreateModel())
							{
								var exchange = "app-logging";
								var consumer = new QueueingBasicConsumer(m);
								var props = new Dictionary<string, object>()
									{
										{"x-expires", 30*60000} // expire queue after 30 minutes, see http://www.rabbitmq.com/extensions.html
									};

								m.ExchangeDeclare(exchange, ExchangeType.Topic);
								var q = m.QueueDeclare("", false, true, false, props);
								m.QueueBind(q, exchange, "#");
								m.BasicConsume(q, true, consumer);

								while (true)
								{
									var msg = (BasicDeliverEventArgs) consumer.Queue.Dequeue();

									if (msg.BasicProperties.IsAppIdPresent())
										Console.Write(msg.BasicProperties.AppId + " ");

									var asUtf8String = msg.Body.AsUtf8String();
									Console.WriteLine(asUtf8String);
									WriteToFile(asUtf8String);
								}
							}
						}
						catch (BrokerUnreachableException)
						{
							Console.WriteLine("Could not connect, retrying...");
							Thread.Sleep(1000);
						}
					}
				}
				// sometimes this happens if rabbit is shut down
				catch (EndOfStreamException)
				{
					Console.WriteLine("RabbitMQ went down, re-connecting...");
				}
				// this happens most of the time
				catch(IOException)
				{
					Console.WriteLine("RabbitMQ went down ungracefully, re-connecting...");
				}
				// this happens when one kills erlang (killall -9 erl)
				catch(OperationInterruptedException)
				{
					Console.WriteLine("Yet another one of RabbitMQ's failure modes - re-connecting...");
				}
			}
		}

		private static void WriteToFile(string body)
		{
			lock (_locker)
			{
				File.AppendAllLines("log.txt", new[]{body});
			}
		}
	}

	static class Extensions
	{
		public static string AsUtf8String(this byte[] args)
		{
			return Encoding.UTF8.GetString(args);
		}
	}
}
