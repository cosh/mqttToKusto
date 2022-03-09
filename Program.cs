using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Text;
using System.Collections.Concurrent;
using MQTTnet.Extensions.ManagedClient;
using MQTTnet.Client.Options;
using MQTTnet;
using Kusto.Ingest;
using Kusto.Data;
using System.Diagnostics;
using System.IO;
using Microsoft.Extensions.Configuration;
using System.Collections.Generic;
using Microsoft.Extensions.Logging;
using Azure.Identity;

namespace mqttToKusto
{
    internal class Program
    {
        static void Main(string[] args)
        {
            Settings settings = GetSettings();

            using var loggerFactory = LoggerFactory.Create(builder =>
            {
                builder
                    .AddFilter("Microsoft", LogLevel.Warning)
                    .AddFilter("System", LogLevel.Warning)
                    .AddFilter("mqttToKusto", LogLevel.Debug)
                    .AddConsole();

                if (!String.IsNullOrWhiteSpace(settings.APPINSIGHTS_INSTRUMENTATIONKEY))
                {
                    builder.AddApplicationInsights(settings.APPINSIGHTS_INSTRUMENTATIONKEY);
                }
            });

            ILogger logger = loggerFactory.CreateLogger<Program>();

            var tasks = new List<Task>();
            var ingestionJobs = new ConcurrentQueue<IngestionJob>();
            var messages = new Dictionary<string, ConcurrentQueue<MqttApplicationMessageReceivedEventArgs>>();
            var topicFilter = new List<MqttTopicFilter>();

            foreach (var aSubscription in settings.Subscriptions)
            {
                var messageQueue = new ConcurrentQueue<MqttApplicationMessageReceivedEventArgs>();
                messages.Add(aSubscription.MqttTopic, messageQueue);

                topicFilter.Add(new MqttTopicFilterBuilder().WithTopic(aSubscription.MqttTopic).Build());

                tasks.Add(Task.Run(() => Batch(logger, aSubscription, messageQueue, ingestionJobs)));
            }

            tasks.Add(Task.Run(() => Subscribe(logger, settings.MqttURL, settings.MqttClientId, messages, topicFilter)));
            tasks.Add(Task.Run(() => SendToKusto(logger, ingestionJobs, settings.Kusto)));

            logger.LogInformation($"Created {tasks.Count} tasks to work on ingestion of {settings.Subscriptions.Count} topics into kusto");

            Task.WaitAll(tasks.ToArray());
        }

        private static Settings GetSettings()
        {
            string developmentConfiguration = "appsettingsDevelopment.json";
            string configFile = "appsettings.json";
            string fileUsedForConfiguration = null;

            if (File.Exists(developmentConfiguration))
            {
                fileUsedForConfiguration = developmentConfiguration;
            }
            else
            {
                fileUsedForConfiguration = configFile;
            }

            IConfiguration config = new ConfigurationBuilder()
                .AddJsonFile(fileUsedForConfiguration)
                .AddEnvironmentVariables()
                .Build();

            Settings settings = config.GetRequiredSection("Settings").Get<Settings>();
            return settings;
        }

        private static void Batch(ILogger logger,
            SettingsSubscription subscription, ConcurrentQueue<MqttApplicationMessageReceivedEventArgs> messages, ConcurrentQueue<IngestionJob> ingestionJobs)
        {
            var timeLimit = subscription.BatchLimitInMinutes * 60 * 1000;

            var sw = Stopwatch.StartNew();

            StringBuilder sb = new StringBuilder();
            int eventCount = 0;

            while (true)
            {
                MqttApplicationMessageReceivedEventArgs mqttEvent;
                while (messages.TryDequeue(out mqttEvent))
                {
                    //still something to add
                    sb.AppendLine(System.Text.Encoding.UTF8.GetString(mqttEvent.ApplicationMessage.Payload));
                    eventCount++;
                }

                //nothing to add any longer, enough for an ingestion command?
                if (eventCount > subscription.BatchLimitNumberOfEvents || sw.ElapsedMilliseconds > timeLimit)
                {
                    if (eventCount > 0)
                    {
                        string tempFile = Path.GetRandomFileName();
                        File.WriteAllText(tempFile, sb.ToString());

                        ingestionJobs.Enqueue(new IngestionJob() { ToBeIngested = tempFile, Subcription = subscription });

                        logger.LogInformation($"Created a file {tempFile} with {eventCount} events for topic {subscription.MqttTopic}");
                    }

                    //prepare for next batch
                    sw.Restart();
                    eventCount = 0;
                    sb.Clear();
                }

                Thread.Sleep(1000);
            }
        }

        private static void SendToKusto(ILogger logger, ConcurrentQueue<IngestionJob> ingestionJobs, SettingsKusto kusto)
        {
            var kustoConnectionStringBuilderEngine =
                new KustoConnectionStringBuilder($"https://{kusto.ClusterName}.kusto.windows.net").WithAadApplicationKeyAuthentication(
                    applicationClientId: kusto.ClientId,
                    applicationKey: kusto.ClientSecret,
                    authority: kusto.TenantId);

            using (IKustoIngestClient client = KustoIngestFactory.CreateDirectIngestClient(kustoConnectionStringBuilderEngine))
            {
                while (true)
                {
                    IngestionJob job;
                    while (ingestionJobs.TryDequeue(out job))
                    {
                        //Ingest from blobs according to the required properties
                        var kustoIngestionProperties = new KustoIngestionProperties(databaseName: kusto.DbName, tableName: job.Subcription.KustoTable);
                        kustoIngestionProperties.SetAppropriateMappingReference(job.Subcription.MappingName, Kusto.Data.Common.DataSourceFormat.multijson);
                        kustoIngestionProperties.Format = Kusto.Data.Common.DataSourceFormat.multijson;

                        logger.LogDebug($"About start ingestion into table {job.Subcription.KustoTable} using file {job.ToBeIngested}");

                        //ingest
                        Ingest(logger, client, job, kustoIngestionProperties, kusto);

                        logger.LogInformation($"Finished ingestion into table {job.Subcription.KustoTable} using file {job.ToBeIngested}");

                        Thread.Sleep(100);

                        File.Delete(job.ToBeIngested);
                        logger.LogDebug($"Deleted file {job.ToBeIngested} because of successful ingestion");

                        Thread.Sleep(10000);
                    }
                }
            }

            static void Ingest(ILogger logger, IKustoIngestClient client, IngestionJob job, KustoIngestionProperties kustoIngestionProperties, SettingsKusto kusto)
            {
                int retries = 0;

                while(retries < kusto.MaxRetries)
                {
                    try
                    {
                        client.IngestFromStorage(job.ToBeIngested, kustoIngestionProperties);
                        return;
                    }
                    catch (Exception e)
                    {
                        logger.LogError(e, $"Could not ingest {job.ToBeIngested} into table {job.Subcription.KustoTable}.");
                        retries++;
                        Thread.Sleep(kusto.MsBetweenRetries);
                    }
                }

            }
        }

        private static async Task Subscribe(ILogger logger, string url, string clientId, Dictionary<string,ConcurrentQueue<MqttApplicationMessageReceivedEventArgs>> messages, List<MqttTopicFilter> topicFilter)
        {
            var options = new ManagedMqttClientOptionsBuilder()
                .WithClientOptions(
                    new MqttClientOptionsBuilder()
                        .WithClientId(clientId)
                        .WithTcpServer(url)
                        //.WithCredentials("user", "pass")
                        .WithCleanSession()
                        .Build()
                )
                .Build();

            var mqttClient = new MqttFactory().CreateManagedMqttClient();

            await mqttClient.SubscribeAsync(topicFilter); 
            mqttClient.UseApplicationMessageReceivedHandler(
                msg => {

                    var topic = msg.ApplicationMessage.Topic;
                    messages[topic].Enqueue(msg);
                }
            );

            mqttClient.UseConnectedHandler(
                (arg) => logger.LogInformation($"Establish connection to {url} {arg.ConnectResult.ResultCode}")
            );

            await mqttClient.StartAsync(options);
        }
    }
}