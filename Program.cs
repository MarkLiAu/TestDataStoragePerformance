// See https://aka.ms/new-console-template for more information

using System.Diagnostics;
using BenchmarkDotNet.Running;
using Microsoft.Identity.Client;
using Microsoft.IdentityModel.Tokens;
using TestDataStoragePerformance;

Console.WriteLine("Hello, World!");

var runner = new RunPerformanceTests();
runner.TestServer = "AzureSqlS2";
runner.ConcurrentTasks = 1;
runner.RecordsCount = 2000;
await runner.SetupTestData();
if (System.Diagnostics.Debugger.IsAttached)
{
    Stopwatch sw = Stopwatch.StartNew();
    await runner.GlobalSetup();
    Console.WriteLine("GlobalSetup:" + sw.ElapsedMilliseconds); sw.Restart();

    await deletetestContainers();

    await runner.SqlReadWithDapper();
    Console.WriteLine("SqlReadWithDapper:" + sw.ElapsedMilliseconds); sw.Restart();
    await runner.SqlReadWithEFNoTracking();
    Console.WriteLine("SqlReadWithEFNoTracking:" + sw.ElapsedMilliseconds); sw.Restart();
    //await runner.SqlReadWithEF();
    //Console.WriteLine("SqlReadWithEF:" + sw.ElapsedMilliseconds); sw.Restart();
    await runner.AzureBlobRead();
    Console.WriteLine("AzureBlobRead:" + sw.ElapsedMilliseconds); sw.Restart();
    //await runner.SqlTruncateAndAddWithEF();
    //Console.WriteLine("SqlTruncateAndAddWithEF:" + sw.ElapsedMilliseconds); sw.Restart();
    //await runner.SqlTruncateAndAddWithEFBulkExtention();
    //Console.WriteLine("SqlTruncateAndAddWithEFBulkExtention:" + sw.ElapsedMilliseconds); sw.Restart();
    await runner.AzureBlobWrite();
    Console.WriteLine("AzureBlobWrite:" + sw.ElapsedMilliseconds); sw.Restart();
}

else 
{
    var result = BenchmarkRunner.Run<RunPerformanceTests>();
}


async Task deletetestContainers()
{
    var containers = await runner.azureStorageRunner.ListContainer();
    Console.WriteLine("total containers:"+containers.Count);
    foreach (var container in containers)
    {
        if (container == "test-data-container") continue;
        //await runner.azureStorageRunner.DeleteContainer(container);
    }

}