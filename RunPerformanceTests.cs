using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;

namespace TestDataStoragePerformance
{
    [SimpleJob(launchCount: 1, warmupCount: 1, iterationCount: 5)]
    public class RunPerformanceTests
    {
        public string data_json_file = @"test-data.json";
        private List<SalesOrderDetail> _salesOrders;
        string localConnString = "";// "Data Source=localhost;Integrated Security=SSPI;Initial Catalog=AdventureWorks2017;MultipleActiveResultSets=true;trustServerCertificate=true";
        string remoteConnString = "Server=tcp:storage-test-svr.database.windows.net,1433;Initial Catalog=AdventureWorks2017;User ID=markli;Password=Sydney@2021!;MultipleActiveResultSets=True;";
        //string azStorageConnString = "DefaultEndpointsProtocol=https;AccountName=storageaccmarktest;AccountKey=xxEPSgVs8KjzPZQAvOFcm3nWZU5cmZyCeCJG/T/iUH8xmvDU8WFqmDCd9FV3dMte0ikU4ftDeAuH+ASt9/RPmw==;EndpointSuffix=core.windows.net";
        string azStorageConnString = "DefaultEndpointsProtocol=https;AccountName=marktestpr;AccountKey=vC9UxFDUmBF7EL+PPPSz/Tb4YIobTRcm62aJM1tcIU35b1/9WRXsfwbJV3pgmnFMqnNHccNHU7Yl+AStKPmQFQ==;EndpointSuffix=core.windows.net";
        string azStorageContainer = "testrun";
        private string azContainerForRead = "test-data-container";
        private string test_blob_file = "test-data.txt";

        public SqlReadAndWrite sqlRunner;
        public AzureStorageReadWrite azureStorageRunner;

        public string TestId=string.Empty;

        [Params("AzureSql-S2")]
        //[Params("LocalSql")]
        public string TestServer;

        [Params(2000)]
        public int RecordsCount;

        [Params(1,5)]
        public int ConcurrentTasks;

        [IterationSetup]
        public void IterationSetup()
        {
            Console.WriteLine($"IterationSetup start {TestId},{TestServer},{RecordsCount},{ConcurrentTasks}");
        }

        [GlobalSetup]
        public async Task GlobalSetup()
        {
            TestId = DateTime.Now.ToString("yyyyMMdd");
            Console.WriteLine($"GlobalSetup start {TestId},{TestServer},{RecordsCount},{ConcurrentTasks}");
            var jsondata = File.ReadAllText(data_json_file);
            _salesOrders = JsonSerializer.Deserialize<List<SalesOrderDetail>>(jsondata).Take(RecordsCount).ToList();

            sqlRunner = new SqlReadAndWrite(GetConnString(TestServer));
            await sqlRunner.ReadWithDapper();
            await sqlRunner.ReadSalesOrderDetailsEFNoTracking();


            azStorageContainer = $"testrun-{RecordsCount}-{TestId}";
            azureStorageRunner = new AzureStorageReadWrite(azStorageConnString);
            var containerExists = await azureStorageRunner.ContainerExists(azStorageContainer);
            if (!containerExists)
            {
                await azureStorageRunner.CreateContainer(azStorageContainer);
            }
        }

        [Benchmark]
        public async Task SqlReadWithDapper()
        {
            var tasks = Enumerable.Range(1, ConcurrentTasks).Select(x =>
                sqlRunner.ReadWithDapper()
            ).ToArray();
            Task.WaitAll(tasks);
        }

        [Benchmark]
        public async Task SqlReadWithEFNoTracking()
        {
            var tasks = Enumerable.Range(1, ConcurrentTasks).Select(x =>
                sqlRunner.ReadSalesOrderDetailsEFNoTracking()
            ).ToArray();
            Task.WaitAll(tasks);
        }

        [Benchmark]
        public async Task SqlReadWithEF()
        {
            var tasks = Enumerable.Range(1, ConcurrentTasks).Select(x =>
                sqlRunner.ReadSalesOrderDetailsEF()
            ).ToArray();
            Task.WaitAll(tasks);
        }

        [Benchmark]
        public async Task AzureBlobRead()
        {
            var tasks = Enumerable.Range(1, ConcurrentTasks).Select(x =>
                azureStorageRunner.ReadBlob(azContainerForRead, GetBlobTestFileName(RecordsCount, x))
                ).ToArray();
            Task.WaitAll(tasks);
        }


        [Benchmark]
        public async Task SqlTruncateAndAddWithEF()
        {
            await sqlRunner.TruncateSalesOrderDetails();
            var tasks = Enumerable.Range(1, ConcurrentTasks).Select(x =>
                sqlRunner.WriteSalesOrderDetails(_salesOrders)
            ).ToArray();
            Task.WaitAll(tasks);
        }

        [Benchmark]
        public async Task SqlTruncateAndAddWithEFBulkExtention()
        {
            await sqlRunner.TruncateSalesOrderDetails();
            var tasks = Enumerable.Range(1, ConcurrentTasks).Select(x =>
                    sqlRunner.BulkInsertSalesOrderDetails(_salesOrders)
            ).ToArray();
            Task.WaitAll(tasks);
        }

        [Benchmark]
        public async Task AzureBlobWrite()
        {
            var tasks = Enumerable.Range(1, ConcurrentTasks).Select(x =>
                    azureStorageRunner.WriteBlob(azStorageContainer, "test-" + Guid.NewGuid().ToString() + ".txt", _salesOrders)
            ).ToArray();
            Task.WaitAll(tasks);
        }


        private string GetConnString(string serverType)
        {
            return serverType == "LocalSql" ? localConnString : remoteConnString;
        }

        public string GetBlobTestFileName(int cnt, int seq)
        {
            return  "test-" + cnt.ToString()+"-"+seq.ToString("000")+".json";
        }

        public async Task SetupTestData()
        {
            Console.WriteLine($"{DateTime.Now:O}:SetupTestData start");

            // read data list
            var jsondata = File.ReadAllText(data_json_file);
            _salesOrders = JsonSerializer.Deserialize<List<SalesOrderDetail>>(jsondata).Take(RecordsCount).ToList();


            // insert to local for reading
            if (!string.IsNullOrEmpty(localConnString))
            {
                var sqldb = new SqlReadAndWrite(localConnString);
                await sqldb.TruncateSalesOrderDetails();
                await sqldb.WriteSalesOrderDetails(_salesOrders);
            }

            // insert to remote
            var sqldbRemote = new SqlReadAndWrite(remoteConnString);
            await sqldbRemote.TruncateSalesOrderDetails();
            await sqldbRemote.WriteSalesOrderDetails(_salesOrders);

            Console.WriteLine($"{DateTime.Now:O}:SetupTestData prepare blob files");
            // prepare blob files for read
            var azureStorageReadWrite = new AzureStorageReadWrite(azStorageConnString);
            if (!await azureStorageReadWrite.ContainerExists(azContainerForRead))
            {
                await azureStorageReadWrite.CreateContainer(azContainerForRead);
                for (int i = 1; i <= 20; i++)
                {
                    await azureStorageReadWrite.WriteBlob(azContainerForRead, GetBlobTestFileName(_salesOrders.Count, i), _salesOrders);
                }
            }
            Console.WriteLine($"{DateTime.Now:O}:SetupTestData finish");

        }
    }
}
