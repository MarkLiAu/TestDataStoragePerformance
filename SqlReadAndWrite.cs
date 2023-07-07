using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Dapper;
using EFCore.BulkExtensions;
using Microsoft.EntityFrameworkCore;

namespace TestDataStoragePerformance
{
    public class SqlReadAndWrite
    {
        private string _connString ;

        public SqlReadAndWrite(string connString)
        {
            _connString=connString;
        }

        public async Task<IList<T>> ReadSalesOrderDetails<T>(string queryString)
        {
            await using SqlConnection conn = new SqlConnection(_connString);
            var result = await conn.QueryAsync<T>(queryString);
            conn.Close();
            return result.ToList();
        }
        public async Task<IList<SalesOrderDetail>> ReadSalesOrderDetailsEF()
        {
            await using SqlEfCore sqldb = new SqlEfCore(_connString);
            var result= sqldb.SalesOrderDetail.ToList();
            sqldb.Dispose();
            return result;
        }

        public async Task<IList<SalesOrderDetail>> ReadSalesOrderDetailsEFNoTracking()
        {
            await using SqlEfCore sqldb = new SqlEfCore(_connString);
            var result = sqldb.SalesOrderDetail.AsNoTracking().ToList();
            sqldb.Dispose();
            return result;
        }

        public async Task<int> WriteSalesOrderDetails(IList<SalesOrderDetail> salesOrderDetails)
        {
            await using SqlEfCore sqldb = new SqlEfCore(_connString);
            sqldb.SalesOrderDetail.AddRange(salesOrderDetails);
            var result = await sqldb.SaveChangesAsync();
            sqldb.Dispose();
            return result;
        }
        public async Task<bool> TruncateSalesOrderDetails()
        {
            await using SqlEfCore sqldb = new SqlEfCore(_connString);
            var result = await sqldb.Database.ExecuteSqlRawAsync("TRUNCATE TABLE SalesOrderDetailTest");
            sqldb.Dispose();
            return true;
        }
        public async Task<bool> ClearSalesOrderDetails()
        {
            await using SqlEfCore sqldb = new SqlEfCore(_connString);
            var result = await sqldb.Database.ExecuteSqlRawAsync("DELETE FROM SalesOrderDetailTest");
            sqldb.Dispose();
            return true;
        }
        public async Task<int> BulkInsertSalesOrderDetails(IList<SalesOrderDetail> salesOrderDetails)
        {
            await using SqlEfCore sqldb = new SqlEfCore(_connString);
            await sqldb.BulkInsertAsync(salesOrderDetails);
            return 1;
        }


        public async Task<IList<SalesOrderDetail>> ReadWithDapper()
        {
            var sql = "SELECT * FROM [SalesOrderDetailTest]";
            return await ReadSalesOrderDetails<SalesOrderDetail>( sql);
        }

        public async Task<IList<SalesOrderDetail>> ReadBackupDataWithDapper()
        {
            var sql = "SELECT * FROM [SalesOrderDetailBak]";
            return await ReadSalesOrderDetails<SalesOrderDetail>(sql);
        }
        
    }
}
