using IoTHubTrigger = Microsoft.Azure.WebJobs.EventHubTriggerAttribute;

using Microsoft.Azure.WebJobs;
using Microsoft.Azure.EventHubs;
using System.Text;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Linq;
using Microsoft.Extensions.Configuration;
using System.Data.SqlClient;
using System;
using System.Globalization;

namespace RuuviTagDataFunction
{
    public static class RuuviTagData
    {
        [FunctionName("RuuviTagDataParser")]
        public static void Run(
            [IoTHubTrigger("messages/events", Connection = "ConnectionString")] EventData message,
            ILogger log, ExecutionContext context)
        {
            // get data and parse as JSON
            var jsonData = JObject.Parse(Encoding.UTF8.GetString(message.Body.Array));
            var deviceId = message.SystemProperties["iothub-connection-device-id"].ToString();
            var timestamp = message.SystemProperties["iothub-enqueuedtime"].ToString();

            // fix timestamp format to universal datetime format for SQL DateTime compatibility
            timestamp = DateTime.Parse(timestamp).ToString("s");

            log.LogInformation("{0} Received from {1}: {2}", timestamp, deviceId, jsonData.ToString());

            try
            {
                // write data to Azure SQL
                var config = new ConfigurationBuilder()
                                .SetBasePath(context.FunctionAppDirectory)
                                .AddJsonFile("local.settings.json", optional: true, reloadOnChange: true)
                                .AddEnvironmentVariables()
                                .Build();

                var sqlConnection = config.GetConnectionString("SqlConnection");

                // SQL Table structure:
                // timestamp: datetime 
                // deviceId: varchar(16)
                // identifier: char
                // temperature: float
                // humidity: float
                // pressure: float
                using (SqlConnection connection = new SqlConnection(sqlConnection))
                {
                    connection.Open();
                    StringBuilder sb = new StringBuilder();

                    sb.Append("INSERT INTO RuuviTagData VALUES (");
                    sb.Append("'" + timestamp + "', ");
                    sb.Append("'" + deviceId + "', ");
                    sb.Append("'" + jsonData["identifier"].ToString() + "', ");
                    sb.Append(jsonData["temperature"].ToString() + ", ");
                    sb.Append(jsonData["humidity"].ToString() + ", ");
                    sb.Append(jsonData["pressure"].ToString());
                    sb.Append(")");

                    log.LogInformation(sb.ToString());
                    SqlCommand cmd = new SqlCommand(sb.ToString(), connection);
                    cmd.ExecuteNonQuery();
                    connection.Close(); 
                }
            }
            catch (Exception ex)
            {
                log.LogInformation("Error: " + ex.ToString());
            }
        }
    }
}