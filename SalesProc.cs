//
// This is not completed yet. Semi-working code to process CSV files from blob storage. Triggered by Queue messages.
// It's written originally to test the Function App processing performance of large CSV files of size around 180MB
// CSV Helper: https://joshclose.github.io/CsvHelper/
//
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using CsvHelper;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Configuration.AzureAppConfiguration;

namespace SalesProcessorFunc
{
    class DataRecord
    {
        public String Region { get; set; }
        public String Country { get; set; }
        public String ItemType { get; set; }
        public String SalesChannel { get; set; }
        public String OrderPriority { get; set; }
        public String OrderDate { get; set; }
        public String OrderID { get; set; }
        public String ShipDate { get; set; }
        public String UnitsSold { get; set; }
        public String UnitPrice { get; set; }
        public String UnitCost { get; set; }
        public String TotalRevenue { get; set; }
        public String TotalCost { get; set; }
        public String TotalProfit { get; set; }
    }

    public static class SalesProc
    {
        private static IConfiguration Configuration { set; get; }
        private static IConfigurationRefresher ConfigurationRefresher { set; get; }

        static SalesProc()
        {
            var builder = new ConfigurationBuilder();
            builder.AddAzureAppConfiguration(options =>
            {
                options.Connect(Environment.GetEnvironmentVariable("ConnectionString"))
                       .ConfigureRefresh(refreshOptions =>
                            refreshOptions.Register("SalesProc:Transform:CSV")
                                          .SetCacheExpiration(TimeSpan.FromSeconds(60))
                );
                ConfigurationRefresher = options.GetRefresher();
            });
            Configuration = builder.Build();
        }

        [FunctionName("SalesProc")]
        public static void Run(
            [QueueTrigger("myqueue", Connection = "AzureWebJobsStorage")] string myQueueItem,
            [Blob("sales-in/{queueTrigger}", FileAccess.Read, Connection = "AzureWebJobsStorage")] Stream myInputBlob,
            [Blob("sales-out/PROCESSED-{queueTrigger}", FileAccess.Write, Connection = "AzureWebJobsStorage")] Stream myOutputBlob,
            ILogger log)
        {
            log.LogInformation($"Sales Queue trigger function processed: {myQueueItem}");
            log.LogInformation($"Input blob\n Name:{myQueueItem} \n Size: {myInputBlob.Length} bytes");

            ConfigurationRefresher.RefreshAsync();

            string keyName = "SalesProc:Transform:CSV";
            string switchTransform = Configuration[keyName];

            // Use the key-value from the App Configuration store
            if (String.Equals(switchTransform, "on"))
            {
                using (var sr = new StreamReader(myInputBlob))
                {
                    using (var sw = new StreamWriter(myOutputBlob))
                    {
                        var reader = new CsvReader(sr, CultureInfo.InvariantCulture);
                        reader.Configuration.HeaderValidated = null;
                        reader.Configuration.MissingFieldFound = null;

                        var writer = new CsvWriter(sw, CultureInfo.InvariantCulture);

                        //CSVReader will now read the whole file into an enumerable
                        //IEnumerable<DataRecord> records = reader.GetRecords<DataRecord>();
                        IEnumerable<DataRecord> records = reader.GetRecords<DataRecord>().ToList();

                        //Write the entire contents of the CSV file into another
                        writer.WriteRecords(records);

                        //First 5 records in CSV file will be printed to the Output Window
                        //foreach (DataRecord record in records.Take(5))
                        foreach (DataRecord record in records)
                        {
                            //log.LogInformation($"===== Region:{record.Region} ===== Country: {record.Country}\n");

                            //Write entire current record
                            writer.WriteRecord(record);

                            //write record field by field
                            writer.WriteField(record.Region);
                            writer.WriteField(record.Country);
                            //writer.WriteField(record.ItemType);
                            //writer.WriteField(record.SalesChannel);
                            //writer.WriteField(record.OrderPriority);
                            //writer.WriteField(record.OrderDate);
                            //writer.WriteField(record.OrderID);
                            //writer.WriteField(record.ShipDate);
                            //writer.WriteField(record.UnitsSold);
                            //writer.WriteField(record.UnitPrice);
                            //writer.WriteField(record.UnitCost);
                            //writer.WriteField(record.TotalRevenue);
                            //writer.WriteField(record.TotalCost);
                            //writer.WriteField(record.TotalProfit);
                            //ensure you write end of record when you are using WriteField method
                            writer.NextRecord();
                        }
                    }
                }
            }

            log.LogInformation($"===== DONE =====\n");
        }
    }
}
