using Azure.Core;
using Azure.Identity;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Consumer;
using Azure.Messaging.EventHubs.Processor;
using Azure.Storage.Blobs;
using ICSharpCode.SharpZipLib.Zip.Compression;
using ICSharpCode.SharpZipLib.Zip.Compression.Streams;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Org.BouncyCastle.Asn1.X509;
using Org.BouncyCastle.Crypto;
using Org.BouncyCastle.Crypto.Generators;
using Org.BouncyCastle.Crypto.Operators;
using Org.BouncyCastle.Math;
using Org.BouncyCastle.OpenSsl;
using Org.BouncyCastle.Pkcs;
using Org.BouncyCastle.Security;
using Org.BouncyCastle.Utilities;
using Org.BouncyCastle.X509;
using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace IoTMetricsToLogAnalytics
{
    static class Program
    {
        private static string ehubNamespaceConnectionString;
        private static string eventHubName;
        private static string consumerGroup;
        private static string blobStorageConnectionString;
        private static string blobContainerName;
        private static string IoTHubResourceId;
        public static string LogAnalyticsWorkspaceId;
        public static string LogAnalyticsWorkspaceKey;

        public static readonly string VersionNumber = "0.1.2.0";
        public static readonly string MetricOrigin = "iot.azm.ms";
        public static readonly string MetricNamespace = "metricsmodule";
        public static readonly string LogAnalyticsLogType = "Insights";
        public static readonly string MetricComputer = System.Environment.MachineName;
        public static readonly string MetricUploadIPName = "IotInsights";
        public static readonly string MetricUploadDataType = "INSIGHTS_METRICS_BLOB";
        public static readonly string IoTUploadMessageIdentifier = "origin-iotedge-metrics-collector";
        public static readonly int UploadMaxRetries = 3;

        public static readonly string WorkspaceDomainSuffix = "azure.com";
        public static readonly string WorkspaceApiVersion = "2016-04-01";
        public const string DefaultLogAnalyticsWorkspaceDomainPrefixOds = ".ods.opinsights.";
        public const string DefaultLogAnalyticsWorkspaceDomainPrefixOms = ".oms.opinsights.";
        public const string ProductInfo = "IoTEdgeMetricsCollectorModule";

        private static AzureLogAnalytics _azureLogAnalytics { get; set; }
        private static int _postSizeInMB = 1;

        public static async Task Main(string[] args)
        {
            Console.WriteLine(DateTime.Now.ToString() + " Starting");
            try
            {
                JObject configuration = JObject.Parse(File.ReadAllText("configuration.json"));
                if (configuration.ContainsKey("ehubNamespaceConnectionString")) { 
                    ehubNamespaceConnectionString = configuration["ehubNamespaceConnectionString"].ToString();
                }
                
                if (configuration.ContainsKey("eventHubName"))
                {
                    eventHubName = configuration["eventHubName"].ToString();
                } else {
                    eventHubName = ehubNamespaceConnectionString.Split(";EntityPath=")[1].ToString();
                }

                if (configuration.ContainsKey("consumerGroup"))
                {
                    consumerGroup = configuration["consumerGroup"].ToString();
                }
                else
                {
                    consumerGroup = EventHubConsumerClient.DefaultConsumerGroupName;
                }

                if (configuration.ContainsKey("blobStorageConnectionString"))
                {
                    blobStorageConnectionString = configuration["blobStorageConnectionString"].ToString();
                }
                
                if (configuration.ContainsKey("blobContainerName"))
                {
                    blobContainerName = configuration["blobContainerName"].ToString();
                }
                
                if (configuration.ContainsKey("IoTHubResourceId"))
                {
                    IoTHubResourceId = configuration["IoTHubResourceId"].ToString();
                }
                
                if (configuration.ContainsKey("LogAnalyticsWorkspaceId")){
                    LogAnalyticsWorkspaceId = configuration["LogAnalyticsWorkspaceId"].ToString();
                }
                
                if (configuration.ContainsKey("LogAnalyticsWorkspaceKey"))
                {
                    LogAnalyticsWorkspaceKey = configuration["LogAnalyticsWorkspaceKey"].ToString();
                }                
            } catch
            {
                Console.WriteLine(DateTime.Now.ToString() + " Error opening configuration.json");
            }

            if (Environment.GetEnvironmentVariable("ehubNamespaceConnectionString") != null)
            {
                ehubNamespaceConnectionString = Environment.GetEnvironmentVariable("ehubNamespaceConnectionString").ToString();
            }

            if (Environment.GetEnvironmentVariable("eventHubName") != null)
            {
                eventHubName = Environment.GetEnvironmentVariable("eventHubName").ToString();
            }

            if (Environment.GetEnvironmentVariable("consumerGroup") != null)
            {
                consumerGroup = Environment.GetEnvironmentVariable("consumerGroup").ToString();
            }

            if (Environment.GetEnvironmentVariable("blobStorageConnectionString") != null)
            {
                blobStorageConnectionString = Environment.GetEnvironmentVariable("blobStorageConnectionString").ToString();
            }

            if (Environment.GetEnvironmentVariable("blobContainerName") != null)
            {
                blobContainerName = Environment.GetEnvironmentVariable("blobContainerName").ToString();
            }

            if (Environment.GetEnvironmentVariable("IoTHubResourceId") != null)
            {
                IoTHubResourceId = Environment.GetEnvironmentVariable("IoTHubResourceId").ToString();
            }

            if (Environment.GetEnvironmentVariable("LogAnalyticsWorkspaceId") != null)
            {
                LogAnalyticsWorkspaceId = Environment.GetEnvironmentVariable("LogAnalyticsWorkspaceId").ToString();
            }

            if (Environment.GetEnvironmentVariable("LogAnalyticsWorkspaceKey") != null)
            {
                LogAnalyticsWorkspaceKey = Environment.GetEnvironmentVariable("LogAnalyticsWorkspaceKey").ToString();
            }

            BlobContainerClient storageClient = new BlobContainerClient(blobStorageConnectionString, blobContainerName);
            EventProcessorClient processor = new EventProcessorClient(storageClient, consumerGroup, ehubNamespaceConnectionString, eventHubName);

            processor.ProcessEventAsync += ProcessEventHandler;
            processor.ProcessErrorAsync += ProcessErrorHandler;

            _azureLogAnalytics = new AzureLogAnalytics(new HttpClient(), new CertGenerator());

            // Start the processing
            await processor.StartProcessingAsync();

            while (processor.IsRunning)
            {
                Thread.Sleep(new TimeSpan(0, 15, 0));
            }

            Console.WriteLine(DateTime.Now.ToString() + " Ending");
        }

        public static Task WhenCancelled(CancellationToken cancellationToken)
        {
            var tcs = new TaskCompletionSource<bool>();
            cancellationToken.Register(s => ((TaskCompletionSource<bool>)s).SetResult(true), tcs);
            return tcs.Task;
        }

        public static async Task ProcessEventHandler(ProcessEventArgs eventArgs)
        {
            if (eventArgs.Data.Properties.ContainsKey("id") &&
                eventArgs.Data.Properties["id"].ToString() == IoTUploadMessageIdentifier) {

                List<IoTHubMetric> iotHubMetricsList = new List<IoTHubMetric>() { };

                string metricsString = string.Empty;
                try
                {
                    metricsString = GZipCompression.Decompress(eventArgs.Data.Body.ToArray());
                }
                catch
                {
                    metricsString = Encoding.UTF8.GetString(eventArgs.Data.Body.ToArray());
                }
                // Cast metric events
                iotHubMetricsList.AddRange(JsonConvert.DeserializeObject<IoTHubMetric[]>(metricsString));

                // Post metrics to Log Analytics
                PublishToFixedTableAsync(iotHubMetricsList).Wait();
                string iothubconnectiondeviceid = eventArgs.Data.SystemProperties["iothub-connection-device-id"].ToString();
                string iothubconnectionmoduleid = eventArgs.Data.SystemProperties["iothub-connection-module-id"].ToString();
                Console.WriteLine(DateTime.Now.ToString() + " Published " + iotHubMetricsList.Count.ToString() + " metrics from " + iothubconnectiondeviceid 
                                    + "\\" + iothubconnectionmoduleid + " sent: " + eventArgs.Data.SystemProperties["iothub-enqueuedtime"].ToString()
                                    + " to Workspace: " + LogAnalyticsWorkspaceId);
            }

            // writing the checkpoint
            await eventArgs.UpdateCheckpointAsync(eventArgs.CancellationToken);
        }

        private static Task ProcessErrorHandler(ProcessErrorEventArgs arg)
        {
            throw new NotImplementedException();
        }

        private static async Task PublishToFixedTableAsync(IEnumerable<IoTHubMetric> metrics)
        {
            try
            {
                IEnumerable<LaMetric> metricsToUpload = metrics.Select(m => new LaMetric(m, string.Empty));
                
                List<List<LaMetric>> metricsChunks = _azureLogAnalytics.CreateContentChunks<LaMetric>(metricsToUpload, Program._postSizeInMB * 1024f * 1024f);

                //Console.WriteLine($"Separated {metricsToUpload.Count()} metrics in {metricsChunks.Count()} chunks of {_postSizeInMB} mb");

                bool success = false;
                for (int i = 0; i < metricsChunks.Count; i++)
                {
                    //Console.WriteLine($"Submitting chunk {i + 1} out of {metricsChunks.Count} with {metricsChunks[i].Count} metrics");

                    // retry loop
                    LaMetricList metricList = new LaMetricList(metricsChunks[i]);
                    for (int r = 0; r < UploadMaxRetries && (!success); r++)
                        success = await _azureLogAnalytics.PostToInsightsMetricsAsync(JsonConvert.SerializeObject(metricList), IoTHubResourceId, true);

                    if (success)
                        //Console.WriteLine($"Successfully sent {metricList.DataItems.Count()} metrics to fixed set table");
                        success = true;
                    else
                        Console.WriteLine($"Failed to sent {metricList.DataItems.Count()} metrics to fixed set table after {UploadMaxRetries} retries");
                }
            }
            catch (Exception e)
            {
                Console.WriteLine($"PublishToFixedTableAsync failed with the following exception: {e}");
            }
        }
    }

    public class AzureLogAnalytics
    {
        private HttpClient _client { get; set; }
        private string _LogAnalyticsWorkspaceId { get; set; }
        private string _LogAnalyticsWorkspaceKey { get; set; }
        private string _workspaceDomainSuffix { get; set; }
        private string _apiVersion { get; set; }
        private CertGenerator _certGenerator { get; set; }
        private X509Certificate2 cert;
        private int failurecount = 0;
        private DateTime lastFailureReportedTime = DateTime.UnixEpoch;

        public AzureLogAnalytics(HttpClient client, CertGenerator certGenerator)
        {
            _client = client;
            _LogAnalyticsWorkspaceId = Program.LogAnalyticsWorkspaceId;
            _LogAnalyticsWorkspaceKey = Program.LogAnalyticsWorkspaceKey;
            _workspaceDomainSuffix = Program.WorkspaceDomainSuffix;
            _apiVersion = Program.WorkspaceApiVersion;
            _certGenerator = certGenerator;
        }

        /// <summary>
        /// Sends data to a custom logs table in Log Analytics
        /// <param name="content"> HTTP request content string </param>
        /// <param name="logType"> Custom log table name </param>
        /// <param name="armResourceId"> Azure ARM resource ID </param>
        /// <returns>
        /// True on success, false on failure.
        /// </returns>
        /// </summary>
        public bool PostToCustomTable(string content, string logType, string armResourceid)
        {
            try
            {
                string requestUriString = $"https://{_LogAnalyticsWorkspaceId}{Program.DefaultLogAnalyticsWorkspaceDomainPrefixOds}{_workspaceDomainSuffix}/api/logs?api-version={_apiVersion}";
                string dateString = DateTime.UtcNow.ToString("r");
                string signature = GetSignature("POST", content.Length, "application/json", dateString, "/api/logs");

                HttpWebRequest request = (HttpWebRequest)WebRequest.Create(requestUriString);

                request.ContentType = "application/json";
                request.Method = "POST";
                request.Headers["x-ms-date"] = dateString;
                request.Headers["x-ms-AzureResourceId"] = armResourceid;
                request.Headers["Authorization"] = signature;
                request.Headers["Log-Type"] = logType;

                byte[] contentBytes = Encoding.UTF8.GetBytes(content);
                using (Stream requestStreamAsync = request.GetRequestStream())
                {
                    requestStreamAsync.Write(contentBytes, 0, contentBytes.Length);
                }
                using (HttpWebResponse responseAsync = (HttpWebResponse)request.GetResponse())
                {
                    if (responseAsync.StatusCode != HttpStatusCode.OK && responseAsync.StatusCode != HttpStatusCode.Accepted)
                    {
                        Stream responseStream = responseAsync.GetResponseStream();
                        if (responseStream != null)
                        {
                            using (StreamReader streamReader = new StreamReader(responseStream))
                            {
                                throw new Exception(streamReader.ReadToEnd());
                            }
                        }
                    }
                }

                return true;
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                if (e.InnerException != null)
                {
                    Console.WriteLine(DateTime.Now.ToString() + " InnerException - " + e.InnerException.Message);
                }
            }

            return false;
        }

        /// <summary>
        /// Sends data to the InsightsMetrics table in Log Analytics
        /// <returns>
        /// True on success, false on failure.
        /// </returns>
        /// </summary>
        public async Task<bool> PostToInsightsMetricsAsync(string content, string armResourceId, bool compressForUpload)
        {
            try
            {
                // Lazily generate and register certificate.
                if (cert == null)
                {
                    (X509Certificate2 tempCert, (string certString, byte[] certBuf), string keyString) = _certGenerator.RegisterAgentWithOMS(Program.DefaultLogAnalyticsWorkspaceDomainPrefixOms);
                    cert = tempCert;
                }

                using (var handler = new HttpClientHandler())
                {
                    handler.ClientCertificates.Add(cert);
                    handler.SslProtocols = System.Security.Authentication.SslProtocols.Tls12;
                    handler.PreAuthenticate = true;
                    handler.ClientCertificateOptions = ClientCertificateOption.Manual;

                    Uri requestUri = new Uri("https://" + _LogAnalyticsWorkspaceId + Program.DefaultLogAnalyticsWorkspaceDomainPrefixOds + _workspaceDomainSuffix + "/OperationalData.svc/PostJsonDataItems");

                    using (HttpClient client = new HttpClient(handler))
                    {
                        client.DefaultRequestHeaders.Add("x-ms-date", DateTime.Now.ToString("YYYY-MM-DD'T'HH:mm:ssZ"));  // should be RFC3339 format;
                        client.DefaultRequestHeaders.Add("X-Request-ID", Guid.NewGuid().ToString("B"));  // This is host byte order instead of network byte order, but it doesn't mater here
                        client.DefaultRequestHeaders.Add("User-Agent", "IotEdgeContainerAgent/" + Program.VersionNumber);
                        client.DefaultRequestHeaders.Add("x-ms-AzureResourceId", armResourceId);

                        // TODO: replace with actual version number
                        client.DefaultRequestHeaders.UserAgent.Add(new ProductInfoHeaderValue("IotEdgeContainerAgent", Program.VersionNumber));

                        // optionally compress content before sending
                        int contentLength;
                        HttpContent contentMsg;
                        if (compressForUpload)
                        {
                            byte[] withHeader = ZlibDeflate(Encoding.UTF8.GetBytes(content));
                            contentLength = withHeader.Length;

                            contentMsg = new ByteArrayContent(withHeader);
                            contentMsg.Headers.Add("Content-Encoding", "deflate");
                        }
                        else
                        {
                            contentMsg = new StringContent(content, Encoding.UTF8);
                            contentLength = ASCIIEncoding.Unicode.GetByteCount(content);
                        }

                        if (contentLength > 1024 * 1024)
                        {
                            Console.WriteLine(
                                "HTTP post content greater than 1mb" + " " +
                                "Length - " + contentLength.ToString());
                        }

                        contentMsg.Headers.ContentType = new MediaTypeHeaderValue("application/json");

                        var response = await client.PostAsync(requestUri, contentMsg).ConfigureAwait(false);
                        var responseMsg = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
                        //Console.WriteLine(((int)response.StatusCode).ToString() + " " + response.ReasonPhrase + " " + responseMsg);

                        if ((int)response.StatusCode != 200)
                        {
                            failurecount += 1;

                            if (DateTime.Now - lastFailureReportedTime > TimeSpan.FromMinutes(1))
                            {
                                Console.WriteLine(
                                    "abnormal HTTP response code - " +
                                    "responsecode: " + ((int)response.StatusCode).ToString() + " " +
                                    "reasonphrase: " + response.ReasonPhrase + " " +
                                    "responsemsg: " + responseMsg + " " +
                                    "count: " + failurecount);
                                failurecount = 0;
                                lastFailureReportedTime = DateTime.Now;
                            }

                            // It's possible that the generated certificate is bad, maybe the module has been running for a over a month? (in which case a topology request would be needed to refresh the cert).
                            // Regen the cert on next run just to be safe.
                            cert = null;
                        }
                        return ((int)response.StatusCode) == 200;
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                if (e.InnerException != null)
                {
                    Console.WriteLine(DateTime.Now.ToString() + " InnerException - " + e.InnerException.Message);
                }
            }

            return false;
        }

        public async Task<bool> PostAsync(string content, string armResourceId)
        {
            try
            {
                string dateString = DateTime.UtcNow.ToString("r");
                Uri requestUri = new Uri($"https://{_LogAnalyticsWorkspaceId}.{Program.DefaultLogAnalyticsWorkspaceDomainPrefixOds}.{_workspaceDomainSuffix}/api/logs?api-version={_apiVersion}");
                string signature = GetSignature("POST", content.Length, "application/json", dateString, "/api/logs");

                _client.DefaultRequestHeaders.Add("Authorization", signature);
                _client.DefaultRequestHeaders.Add("Accept", "application/json");
                _client.DefaultRequestHeaders.Add("Log-Type", Program.LogAnalyticsLogType);
                _client.DefaultRequestHeaders.Add("x-ms-date", dateString);
                _client.DefaultRequestHeaders.Add("x-ms-AzureResourceId", armResourceId);

                var contentMsg = new StringContent(content, Encoding.UTF8);
                contentMsg.Headers.ContentType = new MediaTypeHeaderValue("application/json");
                //Console.WriteLine(this._client.DefaultRequestHeaders.ToString() + contentMsg.Headers + contentMsg.ReadAsStringAsync().Result);

                var response = await _client.PostAsync(requestUri, contentMsg).ConfigureAwait(false);
                var responseMsg = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
                //Console.WriteLine(((int)response.StatusCode).ToString() + " " + response.ReasonPhrase + " " + responseMsg);

                return true;
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                return false;
            }
        }

        /// <summary>
        /// Returns authorization HMAC-SHA256 signature.
        /// More info at https://docs.microsoft.com/en-us/azure/azure-monitor/logs/data-collector-api
        /// <param name="method"> HTTP request method </param>
        /// <param name="contentLength"> Content string length </param>
        /// <param name="contentType"> HTTP request content type </param>
        /// <param name="date"> Date string </param>
        /// <param name="resource"> HTP request path </param>
        /// </summary>
        private string GetSignature(string method, int contentLength, string contentType, string date, string resource)
        {
            string message = $"{method}\n{contentLength}\n{contentType}\nx-ms-date:{date}\n{resource}";
            byte[] bytes = Encoding.UTF8.GetBytes(message);
            using (HMACSHA256 encryptor = new HMACSHA256(Convert.FromBase64String(_LogAnalyticsWorkspaceKey)))
            {
                return $"SharedKey {_LogAnalyticsWorkspaceId}:{Convert.ToBase64String(encryptor.ComputeHash(bytes))}";
            }
        }

        /// <summary>
        /// Compresses a byte array using the Zlib format
        /// <param name="input"> Byte array to compress </param>
        /// </summary>
        private static byte[] ZlibDeflate(byte[] input)
        {
            // "Deflate" compression often instead refers to a Zlib format which requies a 2 byte header and checksum (RFC 1950). 
            // The C# built in deflate stream doesn't support this, so use an external library.
            // Hopefully a built-in Zlib stream will be included in .net 5 (https://github.com/dotnet/runtime/issues/2236)
            var deflater = new Deflater(5, false);
            using (var memoryStream = new MemoryStream())
            using (DeflaterOutputStream outStream = new DeflaterOutputStream(memoryStream, deflater))
            {
                outStream.IsStreamOwner = false;
                outStream.Write(input, 0, input.Length);
                outStream.Flush();
                outStream.Finish();
                return memoryStream.ToArray();
            }
        }

        /// <summary>
        /// Breaks a collection of items into smaller chunks based on size requirements
        /// </summary>
        /// <typeparam name="T">Collection type</typeparam>
        /// <param name="content">The collection</param>
        /// <param name="chunkSizeMB">Max size in megabytes</param>
        /// <returns>A nested collection of items</returns>
        public List<List<T>> CreateContentChunks<T>(IEnumerable<T> content, double chunkSizeMB)
        {
            int contentLength = ASCIIEncoding.Unicode.GetByteCount(JsonConvert.SerializeObject(content));
            double chunksCount = Math.Ceiling(contentLength / (chunkSizeMB));

            // get right number of items per chunk
            int itemsPerChunk = Convert.ToInt32(Math.Ceiling(content.Count() / chunksCount));

            // add chunks to final collection
            var chunkCollection = new List<List<T>>() { };
            int count = 0;
            do
            {
                List<T> chunk = content.Skip(count).Take(itemsPerChunk).ToList();
                chunkCollection.Add(chunk);
                count += itemsPerChunk;
            }
            while (count < content.Count());

            return chunkCollection;
        }
    }

    public class CertGenerator
    {
        private string _LogAnalyticsWorkspaceId { get; set; }
        private string _LogAnalyticsWorkspaceKey { get; set; }
        private string _workspaceDomainSuffix { get; set; }
        private string _apiVersion { get; set; }
        CloudCertStore _certStore { get; set; }

        public CertGenerator()
        {
            _LogAnalyticsWorkspaceId = Program.LogAnalyticsWorkspaceId;
            _LogAnalyticsWorkspaceKey = Program.LogAnalyticsWorkspaceKey;
            _workspaceDomainSuffix = Program.WorkspaceDomainSuffix;
            _apiVersion = Program.WorkspaceApiVersion;

            // storage-based certificate management
            // string storageAccountName = configuration["StorageAccountName"];
        }

        internal class Constants
        {
            /// <summary>
            /// constants related to masking the secrets in container environment variable
            /// </summary>
            public const string DEFAULT_LOG_ANALYTICS_WORKSPACE_DOMAIN = "opinsights.azure.com";

            public const string DEFAULT_SIGNATURE_ALOGIRTHM = "SHA256WithRSA";
        }

        internal class CloudCertStore
        {
            // storage-based certificate management
            private readonly string _containerName = "selfsignedcert";
            private readonly string _certBlob = "cert.pem";
            private readonly string _keyBlob = "key.key";
            private readonly string _pwdBlob = "password.txt";
            private readonly BlobContainerClient _containerClient;

            public CloudCertStore(string storageAccountName)
            {
                TokenCredential tokenCredential = new DefaultAzureCredential();

                BlobServiceClient blobServiceClient = new BlobServiceClient(new Uri($"https://{storageAccountName}.blob.core.windows.net"), tokenCredential);
                _containerClient = blobServiceClient.GetBlobContainerClient(_containerName);
                if (!_containerClient.Exists())
                    blobServiceClient.CreateBlobContainer(_containerName);
            }

            public (X509Certificate2, (string, byte[]), string) GetExistingSelfSignedCertificate()
            {
                try
                {
                    // Initialize values
                    X509Certificate2 certificate = null;
                    byte[] certificateBuffer = new byte[] { };
                    string certString = string.Empty;
                    string privateKeyString = string.Empty;

                    // Set local file paths
                    string localPath = Path.GetTempPath();
                    string certPath = $"{Path.GetTempFileName()}.pem";
                    string keyPath = $"{Path.GetTempFileName()}.txt";
                    string pwdPath = $"{Path.GetTempFileName()}.txt";

                    BlobClient blobClient = _containerClient.GetBlobClient(_certBlob);
                    if (blobClient.Exists())
                    {
                        // Get certificate
                        blobClient.DownloadTo(certPath);

                        // Get certificate password
                        blobClient = _containerClient.GetBlobClient(_pwdBlob);
                        blobClient.DownloadTo(pwdPath);
                        string password = File.ReadAllText(pwdPath);

                        certificate = new X509Certificate2(certPath, password);
                        certString = GetCertInPEMFormat(certificate);
                        certificateBuffer = certificate.RawData;

                        // Get private key
                        blobClient = _containerClient.GetBlobClient(_keyBlob);
                        blobClient.DownloadTo(keyPath);
                        privateKeyString = File.ReadAllText(keyPath);
                    }
                    else
                    {
                        certificate = null;
                    }

                    return (certificate, (certString, certificateBuffer), privateKeyString);
                }
                catch (Exception e)
                {
                    Console.WriteLine($"{e}");
                    throw e;
                }
            }
            public DateTimeOffset GetSelfSignedCertificateLastModifiedUtc()
            {
                try
                {
                    BlobClient blobClient = _containerClient.GetBlobClient(_certBlob);
                    if (blobClient.Exists())
                    {
                        var blobProperties = blobClient.GetProperties();
                        return blobProperties.Value.LastModified;
                    }
                    else
                    {
                        return new DateTime();
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine($"{e}");
                    return new DateTime();
                }
            }
        }

        private (X509Certificate2, (string, byte[]), string) CreateSelfSignedCertificate(string agentGuid)
        {
            // storage-based certificate management
            //DateTimeOffset agentCertLastModified = this._certStore.GetSelfSignedCertificateLastModifiedUtc();
            //if (agentCertLastModified > DateTime.UtcNow.AddMinutes(-5))
            //{
            //    (X509Certificate2 agentCert, (string agentCertString, byte[] agentCertBuf), string agentKeyString) = this._certStore.GetExistingSelfSignedCertificate();
            //    return (agentCert, (agentCertString, agentCert.RawData), agentKeyString);
            //}

            var random = new SecureRandom();

            var certificateGenerator = new X509V3CertificateGenerator();

            var serialNumber = BigIntegers.CreateRandomInRange(BigInteger.One, BigInteger.ValueOf(Int64.MaxValue), random);

            certificateGenerator.SetSerialNumber(serialNumber);

            var dirName = string.Format("CN={0}, CN={1}, OU=Microsoft Monitoring Agent, O=Microsoft", _LogAnalyticsWorkspaceId, agentGuid);

            X509Name certName = new X509Name(dirName);

            certificateGenerator.SetIssuerDN(certName);

            certificateGenerator.SetSubjectDN(certName);

            certificateGenerator.SetNotBefore(DateTime.UtcNow.Date);

            certificateGenerator.SetNotAfter(DateTime.UtcNow.Date.AddYears(1));

            const int strength = 2048;

            var keyGenerationParameters = new KeyGenerationParameters(random, strength);

            var keyPairGenerator = new RsaKeyPairGenerator();

            keyPairGenerator.Init(keyGenerationParameters);

            var subjectKeyPair = keyPairGenerator.GenerateKeyPair();

            certificateGenerator.SetPublicKey(subjectKeyPair.Public);

            // Get Private key for the Certificate
            TextWriter textWriter = new StringWriter();
            PemWriter pemWriter = new PemWriter(textWriter);
            pemWriter.WriteObject(subjectKeyPair.Private);
            pemWriter.Writer.Flush();

            string privateKeyString = textWriter.ToString();

            var issuerKeyPair = subjectKeyPair;
            var signatureFactory = new Asn1SignatureFactory(Constants.DEFAULT_SIGNATURE_ALOGIRTHM, issuerKeyPair.Private);
            var bouncyCert = certificateGenerator.Generate(signatureFactory);

            // Lets convert it to X509Certificate2
            X509Certificate2 certificate;

            Pkcs12Store store = new Pkcs12StoreBuilder().Build();

            store.SetKeyEntry($"{agentGuid}_key", new AsymmetricKeyEntry(subjectKeyPair.Private), new[] { new X509CertificateEntry(bouncyCert) });

            string exportpw = Guid.NewGuid().ToString("x");

            using (var ms = new MemoryStream())
            {
                store.Save(ms, exportpw.ToCharArray(), random);
                certificate = new X509Certificate2(ms.ToArray(), exportpw, X509KeyStorageFlags.Exportable);
            }

            //Get Certificate in PEM format
            string certString = GetCertInPEMFormat(certificate);

            return (certificate, (certString, certificate.RawData), privateKeyString);
        }

        // Delete the certificate and key files
        private void DeleteCertificateAndKeyFile()
        {
            File.Delete(Environment.GetEnvironmentVariable("CI_CERT_LOCATION"));
            File.Delete(Environment.GetEnvironmentVariable("CI_KEY_LOCATION"));
        }

        private static string GetCertInPEMFormat(X509Certificate2 certificate)
        {
            //Get Certificate in PEM format
            StringBuilder builder = new StringBuilder();
            builder.AppendLine("-----BEGIN CERTIFICATE-----");
            builder.AppendLine(
                Convert.ToBase64String(certificate.RawData, Base64FormattingOptions.InsertLineBreaks));
            builder.AppendLine("-----END CERTIFICATE-----");
            string certString = builder.ToString();

            return certString;
        }

        private string Sign(string requestdate, string contenthash, string key)
        {
            var signatureBuilder = new StringBuilder();
            signatureBuilder.Append(requestdate);
            signatureBuilder.Append("\n");
            signatureBuilder.Append(contenthash);
            signatureBuilder.Append("\n");
            string rawsignature = signatureBuilder.ToString();

            //string rawsignature = contenthash;

            HMACSHA256 hKey = new HMACSHA256(Convert.FromBase64String(key));
            return Convert.ToBase64String(hKey.ComputeHash(Encoding.UTF8.GetBytes(rawsignature)));
        }

        public void RegisterWithOms(X509Certificate2 cert, string AgentGuid, string logAnalyticsWorkspaceDomainPrefixOms)
        {
            string rawCert = Convert.ToBase64String(cert.GetRawCertData()); //base64 binary
            string hostName = Dns.GetHostName();

            string date = DateTime.Now.ToString("O");

            string xmlContent = "<?xml version=\"1.0\"?>" +
                "<AgentTopologyRequest xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\" xmlns=\"http://schemas.microsoft.com/WorkloadMonitoring/HealthServiceProtocol/2014/09/\">" +
                "<FullyQualfiedDomainName>"
                 + hostName
                + "</FullyQualfiedDomainName>" +
                "<EntityTypeId>"
                    + AgentGuid
                + "</EntityTypeId>" +
                "<AuthenticationCertificate>"
                  + rawCert
                + "</AuthenticationCertificate>" +
                "</AgentTopologyRequest>";

            SHA256 sha256 = SHA256.Create();

            string contentHash = Convert.ToBase64String(sha256.ComputeHash(Encoding.ASCII.GetBytes(xmlContent)));

            string authKey = string.Format("{0}; {1}", _LogAnalyticsWorkspaceId, Sign(date, contentHash, _LogAnalyticsWorkspaceKey));


            HttpClientHandler clientHandler = new HttpClientHandler();

            clientHandler.ClientCertificates.Add(cert);

            var client = new HttpClient(clientHandler);

            string url = "https://" + _LogAnalyticsWorkspaceId + logAnalyticsWorkspaceDomainPrefixOms + _workspaceDomainSuffix + "/AgentService.svc/AgentTopologyRequest";

            Console.WriteLine("OMS endpoint Url : {0}", url);

            client.DefaultRequestHeaders.Add("x-ms-Date", date);
            client.DefaultRequestHeaders.Add("x-ms-version", "August, 2014");
            client.DefaultRequestHeaders.Add("x-ms-SHA256_Content", contentHash);
            client.DefaultRequestHeaders.TryAddWithoutValidation("Authorization", authKey);
            client.DefaultRequestHeaders.Add("user-agent", "MonitoringAgent/OneAgent");
            client.DefaultRequestHeaders.Add("Accept-Language", "en-US");

            HttpContent httpContent = new StringContent(xmlContent, Encoding.UTF8);
            httpContent.Headers.ContentType = new MediaTypeHeaderValue("application/xml");

            Console.WriteLine("sent registration request");
            Task<HttpResponseMessage> response = client.PostAsync(new Uri(url), httpContent);
            Console.WriteLine("waiting response for registration request : {0}", response.Result.StatusCode);
            response.Wait();
            Console.WriteLine("registration request processed");
            Console.WriteLine("Response result status code : {0}", response.Result.StatusCode);
            HttpContent responseContent = response.Result.Content;
            string result = responseContent.ReadAsStringAsync().Result;
            Console.WriteLine("Return Result: " + result);
            Console.WriteLine(response.Result.ToString());
            if (response.Result.StatusCode != HttpStatusCode.OK)
            {
                Console.WriteLine("Deleting SSL certificate and key");
                DeleteCertificateAndKeyFile();
            }
        }

        public void RegisterWithOmsWithBasicRetryAsync(X509Certificate2 cert, string AgentGuid, string logAnalyticsWorkspaceDomainPrefixOms)
        {
            int currentRetry = 0;

            for (; ; )
            {
                try
                {
                    RegisterWithOms(cert, AgentGuid, logAnalyticsWorkspaceDomainPrefixOms);

                    // Return or break.
                    break;
                }
                catch (Exception ex)
                {
                    currentRetry++;

                    // Check if the exception thrown was a transient exception
                    // based on the logic in the error detection strategy.
                    // Determine whether to retry the operation, as well as how
                    // long to wait, based on the retry strategy.
                    if (currentRetry > 3)
                    {
                        // If this isn't a transient error or we shouldn't retry,
                        // rethrow the exception.
                        Console.WriteLine($"exception occurred : {ex}");
                        throw;
                    }
                }

                // Wait to retry the operation.
                // Consider calculating an exponential delay here and
                // using a strategy best suited for the operation and fault.
                Task.Delay(1000);
            }
        }

        public (X509Certificate2 tempCert, (string, byte[]), string) RegisterAgentWithOMS(string logAnalyticsWorkspaceDomainPrefixOms)
        {
            X509Certificate2 agentCert = null;
            string certString;
            byte[] certBuf;
            string keyString;

            var agentGuid = Guid.NewGuid().ToString("B");

            try
            {
                Environment.SetEnvironmentVariable("CI_AGENT_GUID", agentGuid);
            }
            catch (Exception ex)
            {
                Console.WriteLine("Failed to set env variable (CI_AGENT_GUID)" + ex.Message);
            }

            try
            {
                (agentCert, (certString, certBuf), keyString) = CreateSelfSignedCertificate(agentGuid);

                if (agentCert == null)
                {
                    throw new Exception($"creating self-signed certificate failed for agentGuid : {agentGuid} and workspace: {_LogAnalyticsWorkspaceId}");
                }

                Console.WriteLine($"Successfully created self-signed certificate for agentGuid : {agentGuid} and workspace: {_LogAnalyticsWorkspaceId}");

                RegisterWithOmsWithBasicRetryAsync(agentCert, agentGuid, logAnalyticsWorkspaceDomainPrefixOms);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Registering agent with OMS failed (are the Log Analytics Workspace ID and Key correct?) : {ex}");
                Environment.Exit(1);

                // to make the code analyzer happy
                throw new Exception();
            }

            return (agentCert, (certString, certBuf), keyString);
        }
    }

    public class IoTHubMetric
    {
        [JsonProperty("TimeGeneratedUtc")]
        public DateTime TimeGeneratedUtc { get; set; }
        [JsonProperty("Name")]
        public string Name { get; set; }
        [JsonProperty("Value")]
        public double Value { get; set; }
        [JsonProperty("Labels")]
        public IReadOnlyDictionary<string, string> Labels { get; set; }
    }

    public class LaMetricList
    {
        public string DataType => Program.MetricUploadDataType;
        public string IPName => Program.MetricUploadIPName;
        public IEnumerable<LaMetric> DataItems { get; set; }

        public LaMetricList(IEnumerable<LaMetric> items)
        {
            DataItems = items;
        }
    }

    public class LaMetric
    {
        public string Origin { get; set; }
        public string Namespace { get; set; }
        public string Name { get; set; }
        public double Value { get; set; }
        public DateTime CollectionTime { get; set; }
        public string Tags { get; set; }
        public string Computer { get; set; }
        public LaMetric(IoTHubMetric metric, string hostname)
        {
            // forms DB key
            Name = metric.Name;
            Tags = JsonConvert.SerializeObject(metric.Labels);

            // value
            Value = metric.Value;

            // optional 
            CollectionTime = metric.TimeGeneratedUtc;
            Computer = Program.MetricComputer;
            Origin = Program.MetricOrigin;
            Namespace = Program.MetricNamespace;
        }
    }

    public class GZipCompression
    {
        public static string Decompress(Stream compressedStream)
        {
            using GZipStream decompressionStream = new GZipStream(compressedStream, CompressionMode.Decompress);
            StreamReader reader = new StreamReader(decompressionStream);
            string decompressedText = reader.ReadToEnd();

            return decompressedText;
        }

        public static string Decompress(byte[] compressedBytes)
        {
            using Stream compressedStream = new MemoryStream(compressedBytes);
            using GZipStream decompressionStream = new GZipStream(compressedStream, CompressionMode.Decompress);
            using MemoryStream resultStream = new MemoryStream();
            decompressionStream.CopyTo(resultStream);

            byte[] decompressedBytes = resultStream.ToArray();
            string decompressedText = Encoding.UTF8.GetString(decompressedBytes);

            return decompressedText;
        }
    }
}
