using ContactCenterPOC.Models;
using ContactCenterPOC.Services;
using Microsoft.AspNetCore.Mvc;

namespace ContactCenterPOC.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class CampaignController : ControllerBase
    {
        private readonly CampaignService _campaignService;
        private readonly ILogger<CampaignController> _logger;

        public CampaignController(CampaignService campaignService, ILogger<CampaignController> logger)
        {
            _campaignService = campaignService;
            _logger = logger;
        }

        [HttpGet]
        public async Task<IActionResult> GetCampaigns()
        {
            var campaigns = await _campaignService.GetAllAsync();
            return Ok(campaigns);
        }

        [HttpPost]
        public async Task<IActionResult> CreateCampaign([FromBody] CreateCampaignRequest request)
        {
            if (!ModelState.IsValid)
            {
                var errors = ModelState.Values
                    .SelectMany(v => v.Errors)
                    .Select(e => e.ErrorMessage)
                    .ToList();
                return BadRequest(new
                {
                    error = "Validation failed",
                    message = string.Join("; ", errors)
                });
            }

            try
            {
                var campaign = await _campaignService.CreateAsync(request);
                return Created($"/api/Campaign/{campaign.Id}", campaign);
            }
            catch (InvalidOperationException ex)
            {
                return BadRequest(new
                {
                    error = "Campaign creation failed",
                    message = ex.Message
                });
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error creating campaign");
                return StatusCode(500, new
                {
                    error = "Internal error",
                    message = "Failed to create campaign"
                });
            }
        }
        [HttpPut("{id}")]
        public async Task<IActionResult> UpdateCampaign(string id, [FromBody] UpdateCampaignRequest request)
        {
            if (!ModelState.IsValid)
            {
                var errors = ModelState.Values.SelectMany(v => v.Errors).Select(e => e.ErrorMessage).ToList();
                return BadRequest(new { error = "Validation failed", message = string.Join("; ", errors) });
            }

            try
            {
                var campaign = await _campaignService.UpdateAsync(id, request);
                if (campaign == null)
                    return NotFound(new { error = "Campaign not found", message = $"No campaign with ID '{id}'" });
                return Ok(campaign);
            }
            catch (InvalidOperationException ex)
            {
                return BadRequest(new { error = "Update failed", message = ex.Message });
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error updating campaign {Id}", id);
                return StatusCode(500, new { error = "Internal error", message = "Failed to update campaign" });
            }
        }

        [HttpDelete("{id}")]
        public async Task<IActionResult> DeleteCampaign(string id)
        {
            try
            {
                var deleted = await _campaignService.DeleteAsync(id);
                if (!deleted)
                    return NotFound(new { error = "Campaign not found", message = $"No campaign with ID '{id}'" });
                return NoContent();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error deleting campaign {Id}", id);
                return StatusCode(500, new { error = "Internal error", message = "Failed to delete campaign" });
            }
        }

        [HttpPost("reset")]
        public async Task<IActionResult> ResetCampaigns()
        {
            await _campaignService.ResetToDefaultsAsync();
            var campaigns = await _campaignService.GetAllAsync();
            return Ok(new { message = "Campaigns reset to defaults", count = campaigns.Count });
        }
    }
}