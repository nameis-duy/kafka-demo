using KafkaDataExport.Api.Models;
using KafkaDataExport.Api.Services;
using Microsoft.AspNetCore.Mvc;

namespace KafkaDataExport.Api.Controllers;

[ApiController]
[Route("api/[controller]")]
public sealed class ExportController : ControllerBase
{
    private readonly IExportJobService _exportJobService;

    public ExportController(IExportJobService exportJobService)
    {
        _exportJobService = exportJobService;
    }

    [HttpPost("request")]
    public async Task<ActionResult<ExportRequestResponse>> RequestExport(
        [FromBody] ExportRequest request,
        CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(request.CustomerId))
        {
            return BadRequest("CustomerId is required.");
        }

        var response = await _exportJobService.QueueExportAsync(request, cancellationToken);
        return Accepted(response);
    }
}
