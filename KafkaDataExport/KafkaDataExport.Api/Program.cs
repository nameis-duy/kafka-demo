using Confluent.Kafka;
using KafkaDataExport.Api.Models;
using KafkaDataExport.Api.Services;
using Microsoft.Extensions.Options;

var builder = WebApplication.CreateBuilder(args);

builder.Services.Configure<KafkaSettings>(builder.Configuration.GetSection("Kafka"));

builder.Services.AddSingleton<IProducer<Null, string>>(sp =>
{
    var kafkaSettings = sp.GetRequiredService<IOptions<KafkaSettings>>().Value;
    var config = new ProducerConfig
    {
        BootstrapServers = kafkaSettings.BootstrapServers
    };

    return new ProducerBuilder<Null, string>(config).Build();
});

builder.Services.AddScoped<IExportJobService, ExportJobService>();
builder.Services.AddControllers();
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

var app = builder.Build();

if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

app.MapControllers();

app.Run();
