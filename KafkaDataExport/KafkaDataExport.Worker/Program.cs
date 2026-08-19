using Confluent.Kafka;
using KafkaDataExport.Worker.Consumers;
using KafkaDataExport.Worker.Models;
using KafkaDataExport.Worker.Services;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;

var builder = WebApplication.CreateBuilder(args);

builder.Services.Configure<KafkaSettings>(builder.Configuration.GetSection("Kafka"));

builder.Services.AddDbContext<ExportDbContext>(options =>
    options.UseInMemoryDatabase("ExportStore"));

builder.Services.AddSingleton<IProducer<Null, string>>(sp =>
{
    var kafkaSettings = sp.GetRequiredService<IOptions<KafkaSettings>>().Value;
    var config = new ProducerConfig
    {
        BootstrapServers = kafkaSettings.BootstrapServers
    };

    return new ProducerBuilder<Null, string>(config).Build();
});

builder.Services.AddSingleton<CustomerRepository>();
builder.Services.AddSingleton<ExportService>();

builder.Services.AddHostedService<ExportRequestConsumer>();
builder.Services.AddHostedService<ExportDataConsumer>();

var app = builder.Build();

app.MapGet("/exports", async (ExportDbContext db) =>
    await db.ExportData.ToListAsync());

app.MapGet("/exports/{jobId}", async (string jobId, ExportDbContext db) =>
{
    var record = await db.ExportData.FirstOrDefaultAsync(e => e.JobId == jobId);
    return record is null ? Results.NotFound() : Results.Ok(record);
});

app.Run();
