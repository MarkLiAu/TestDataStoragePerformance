using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Serialization;
using Microsoft.EntityFrameworkCore;
using Microsoft.IdentityModel.Tokens;

//using Microsoft.EntityFrameworkCore;

namespace TestDataStoragePerformance
{
    public class SqlEfCore : DbContext
    {
        private string _connString;
        public DbSet<SalesOrderDetail> SalesOrderDetail { get; set; }

        //public SqlEfCore(DbContextOptions options) : base(options) { }

        public SqlEfCore(string connString)
        {
            _connString = connString;
        }

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        {
            optionsBuilder.UseSqlServer(_connString);
            base.OnConfiguring(optionsBuilder);
        }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<SalesOrderDetail>().ToTable("SalesOrderDetailTest");
            base.OnModelCreating(modelBuilder);
        }
    }

}
