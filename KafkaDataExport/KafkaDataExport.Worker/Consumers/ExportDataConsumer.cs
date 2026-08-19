using System.Text.Json;
using Confluent.Kafka;
using KafkaDataExport.Worker.Models;
using KafkaDataExport.Worker.Services;
using Microsoft.Extensions.Options;

namespace KafkaDataExport.Worker.Consumers;

public sealed class ExportDataConsumer : BackgroundService
{
    private readonly ILogger<ExportDataConsumer> _logger;
    private readonly KafkaSettings _kafkaSettings;
    private readonly IServiceScopeFactory _scopeFactory;

    public ExportDataConsumer(
        ILogger<ExportDataConsumer> logger,
        IOptions<KafkaSettings> kafkaOptions,
        IServiceScopeFactory scopeFactory)
    {
        _logger = logger;
        _kafkaSettings = kafkaOptions.Value;
        _scopeFactory = scopeFactory;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = _kafkaSettings.BootstrapServers,
            GroupId = _kafkaSettings.ExportDataGroupId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = true
        };

        using var consumer = new ConsumerBuilder<Ignore, string>(config).Build();
        consumer.Subscribe(_kafkaSettings.ExportDataTopic);

        _logger.LogInformation("ExportDataConsumer subscribed to topic {Topic}", _kafkaSettings.ExportDataTopic);

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                var consumeResult = consumer.Consume(stoppingToken);
                var exportData = JsonSerializer.Deserialize<ExportDataMessage>(consumeResult.Message.Value);

                if (exportData is null)
                {
                    _logger.LogWarning("Received invalid export data message.");
                    continue;
                }

                await using var scope = _scopeFactory.CreateAsyncScope();
                var db = scope.ServiceProvider.GetRequiredService<ExportDbContext>();

                db.ExportData.Add(new ExportDataRecord
                {
                    JobId = exportData.JobId,
                    CustomerId = exportData.CustomerId,
                    Format = exportData.Format,
                    RequestedBy = exportData.RequestedBy,
                    GeneratedAtUtc = exportData.GeneratedAtUtc,
                    TotalRows = exportData.TotalRows
                });

                await db.SaveChangesAsync(stoppingToken);

                _logger.LogInformation(
                    "Export stored for job {JobId}, customer {CustomerId}, rows {Rows}",
                    exportData.JobId,
                    exportData.CustomerId,
                    exportData.TotalRows);
            }
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation("ExportDataConsumer is stopping.");
        }
        finally
        {
            consumer.Close();
        }
    }
}
