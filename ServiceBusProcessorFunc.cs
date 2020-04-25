using System;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;

namespace SalesProcessorFunc
{
    public static class ServiceBusProcessorFunc
    {
        [FunctionName("ServiceBusProcessorFunc")]
        public static void Run([ServiceBusTrigger("myqueue", Connection = "Endpoint=sb://redondosb.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=Iw5N667RqN/QfhFLNrtjnQEskGYb3p3sFLw3xXM+vno=")]string myQueueItem, ILogger log)
        {
            log.LogInformation($"C# ServiceBus queue trigger function processed message: {myQueueItem}");
        }
    }
}
