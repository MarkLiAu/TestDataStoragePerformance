using Azure.Storage.Blobs;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading.Tasks;
using Azure.Identity;
using Azure.Storage.Blobs.Models;
using Azure.Storage;

namespace TestDataStoragePerformance
{
    public class AzureStorageReadWrite
    {
        private string _connString;
        private BlobServiceClient blobServiceClient;

        public AzureStorageReadWrite(string connString)
        {
            _connString = connString;
            blobServiceClient = new BlobServiceClient(_connString);
        }

        public async Task CreateContainer(string containerName)
        {
            await blobServiceClient.CreateBlobContainerAsync(containerName);
        }

        public async Task<bool> ContainerExists(string containerName)
        {
            var containerClient = new BlobContainerClient(_connString, containerName);
            return await containerClient.ExistsAsync();
        }
        public async Task<bool> DeleteContainer(string containerName)
        {
            var containerClient = new BlobContainerClient(_connString, containerName);
            var ret = await containerClient.DeleteAsync();
            Console.WriteLine(containerName+":"+ ret.Status);
            return ret.Status == 0;
        }
        public async Task<List<string>> ListContainer()
        {
            var result = new List<string>();
            var resultSegment =
                blobServiceClient.GetBlobContainersAsync(BlobContainerTraits.Metadata)
                    .AsPages(default, 100);

            await foreach (Azure.Page<BlobContainerItem> containerPage in resultSegment)
            {
                foreach (BlobContainerItem containerItem in containerPage.Values)
                {
                    result.Add(containerItem.Name);
                    Console.WriteLine("Container name: {0}", containerItem.Name);
                }

            }
            return result;
        }


        public async Task WriteBlob(string containerName, string filename, IList<SalesOrderDetail> salesOrderDetails)
        {
            //var pid = $"WriteBlob: {containerName}, {filename}, {salesOrderDetails.Count}, {Guid.NewGuid()}";
            //Console.WriteLine($"{DateTime.Now:O}: {pid} start");

            var containerClient = blobServiceClient.GetBlobContainerClient(containerName);
            var data = BinaryData.FromObjectAsJson(salesOrderDetails);

            var blobClient = containerClient.GetBlobClient(filename);
            var options = new BlobUploadOptions
            {
                TransferOptions = new StorageTransferOptions
                {
                    MaximumConcurrency = 20,
                    MaximumTransferSize = 50 * 1024 * 1024
                }
            };

            await blobClient.UploadAsync(data,options);
            //Console.WriteLine($"{DateTime.Now:O}:{pid} finish");
        }

        public async Task<List<SalesOrderDetail>> ReadBlob(string containerName, string filename)
        {
            //var pid = $"ReadBlob: {containerName}, {filename}, {Guid.NewGuid()}";
            //Console.WriteLine($"{DateTime.Now:O}:{pid} start");

            var containerClient = blobServiceClient.GetBlobContainerClient(containerName);

            var blobClient = containerClient.GetBlobClient(filename);
            var options = new StorageTransferOptions
            {
                MaximumConcurrency = 20,
                MaximumTransferSize = 100 * 1024 * 1024
            };
            var memStream = new MemoryStream();
            var downloadResult = await blobClient.DownloadToAsync(memStream, transferOptions:options);
            memStream.Position = 0;
            //var sr = new StreamReader(memStream);
            //var myStr = sr.ReadToEnd();
            var result = JsonSerializer.Deserialize<List<SalesOrderDetail>>(memStream);
            //Console.WriteLine($"{DateTime.Now:O}:{pid} finish, count:{result.Count}");
            return result;
        }
    }
}
