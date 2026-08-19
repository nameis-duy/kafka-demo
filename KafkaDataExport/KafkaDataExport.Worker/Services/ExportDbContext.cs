using KafkaDataExport.Worker.Models;
using Microsoft.EntityFrameworkCore;

namespace KafkaDataExport.Worker.Services;

public sealed class ExportDbContext : DbContext
{
    public ExportDbContext(DbContextOptions<ExportDbContext> options) : base(options) { }

    public DbSet<ExportDataRecord> ExportData => Set<ExportDataRecord>();
}
