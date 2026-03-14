using Azure.Storage.Blobs;
using ContactCenterPOC.Models;
using System.Text.Json;

namespace ContactCenterPOC.Services
{
    public class CampaignService
    {
        private readonly BlobServiceClient _blobServiceClient;
        private readonly IConfiguration _configuration;
        private readonly ILogger<CampaignService> _logger;
        private readonly string _containerName;
        private readonly string _blobName = "campaigns.json";
        private readonly bool _useLocalFiles;
        private readonly string _localFilePath;
        private List<Campaign> _campaigns = new();
        private bool _initialized = false;
        private readonly SemaphoreSlim _initLock = new(1, 1);

        private static readonly JsonSerializerOptions _writeOptions = new()
        {
            WriteIndented = true,
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase
        };

        private static readonly JsonSerializerOptions _readOptions = new()
        {
            PropertyNameCaseInsensitive = true
        };

        public CampaignService(BlobServiceClient blobServiceClient, IConfiguration configuration, ILogger<CampaignService> logger)
        {
            _blobServiceClient = blobServiceClient;
            _configuration = configuration;
            _logger = logger;
            _containerName = configuration["BlobStorage:ContainerName"] ?? "callcenter-data";
            _useLocalFiles = string.Equals(configuration["Storage:UseLocalFiles"], "true", StringComparison.OrdinalIgnoreCase);
            var dataDir = configuration["Storage:DataDir"] ?? "data";
            _localFilePath = Path.Combine(dataDir, "campaigns.json");
        }

        private async Task EnsureInitializedAsync()
        {
            if (_initialized) return;

            await _initLock.WaitAsync();
            try
            {
                if (_initialized) return;

                await LoadCampaignsAsync();
                _initialized = true;
            }
            finally
            {
                _initLock.Release();
            }
        }

        private async Task LoadCampaignsAsync()
        {
            try
            {
                if (_useLocalFiles)
                {
                    if (File.Exists(_localFilePath))
                    {
                        var json = await File.ReadAllTextAsync(_localFilePath);
                        _campaigns = JsonSerializer.Deserialize<List<Campaign>>(json, _readOptions) ?? new List<Campaign>();
                        _logger.LogInformation("Loaded {Count} campaigns from local file", _campaigns.Count);
                    }
                    else
                    {
                        _logger.LogInformation("No local campaigns file found, loading defaults");
                        _campaigns = GetDefaultCampaigns();
                        await SaveCampaignsAsync();
                    }
                    return;
                }

                var containerClient = _blobServiceClient.GetBlobContainerClient(_containerName);
                await containerClient.CreateIfNotExistsAsync();
                var blobClient = containerClient.GetBlobClient(_blobName);

                if (await blobClient.ExistsAsync())
                {
                    var response = await blobClient.DownloadContentAsync();
                    var json = response.Value.Content.ToString();
                    _campaigns = JsonSerializer.Deserialize<List<Campaign>>(json, _readOptions) ?? new List<Campaign>();
                    _logger.LogInformation("Loaded {Count} campaigns from Blob Storage", _campaigns.Count);
                }
                else
                {
                    _logger.LogInformation("No campaigns blob found, loading defaults");
                    _campaigns = GetDefaultCampaigns();
                    await SaveCampaignsAsync();
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to load campaigns from Blob Storage, using defaults");
                _campaigns = GetDefaultCampaigns();
            }
        }

        private async Task SaveCampaignsAsync()
        {
            try
            {
                var json = JsonSerializer.Serialize(_campaigns, _writeOptions);

                if (_useLocalFiles)
                {
                    var dir = Path.GetDirectoryName(_localFilePath);
                    if (!string.IsNullOrEmpty(dir))
                        Directory.CreateDirectory(dir);
                    await File.WriteAllTextAsync(_localFilePath, json);
                    _logger.LogInformation("Saved {Count} campaigns to local file", _campaigns.Count);
                    return;
                }

                var containerClient = _blobServiceClient.GetBlobContainerClient(_containerName);
                await containerClient.CreateIfNotExistsAsync();
                var blobClient = containerClient.GetBlobClient(_blobName);

                using var stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(json));
                await blobClient.UploadAsync(stream, overwrite: true);
                _logger.LogInformation("Saved {Count} campaigns to Blob Storage", _campaigns.Count);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to save campaigns");
            }
        }

        public async Task ResetToDefaultsAsync()
        {
            _campaigns = GetDefaultCampaigns();
            await SaveCampaignsAsync();
            _logger.LogInformation("Reset campaigns to defaults ({Count} campaigns)", _campaigns.Count);
        }

        public async Task<List<Campaign>> GetAllAsync()
        {
            await EnsureInitializedAsync();
            return _campaigns.ToList();
        }

        public async Task<Campaign?> GetByIdAsync(string id)
        {
            await EnsureInitializedAsync();
            return _campaigns.FirstOrDefault(c => c.Id == id);
        }

        public async Task<Campaign> CreateAsync(CreateCampaignRequest request)
        {
            await EnsureInitializedAsync();

            // Validate title uniqueness
            if (_campaigns.Any(c => c.Title.Equals(request.Title, StringComparison.OrdinalIgnoreCase)))
            {
                throw new InvalidOperationException($"A campaign with title '{request.Title}' already exists.");
            }

            var campaign = new Campaign
            {
                Id = Guid.NewGuid().ToString(),
                Title = request.Title,
                Description = request.Description,
                AiBehaviorInstructions = request.AiBehaviorInstructions,
                IsDefault = false,
                CreatedAt = DateTimeOffset.UtcNow
            };

            _campaigns.Add(campaign);
            await SaveCampaignsAsync();

            _logger.LogInformation("Created campaign '{Title}' with ID {Id}", campaign.Title, campaign.Id);
            return campaign;
        }

        private static List<Campaign> GetDefaultCampaigns()
        {
            static string BuildDefaultPrompt(string agentName, string campaignSpecificInstructions)
            {
                return $@"Bạn là {agentName}, nhân viên tổng đài AI gọi ra cho công ty chúng tôi.

QUY TẮC NHẬN DẠNG:
- Không được nói bạn là người thật.
- Không bao giờ phá vỡ vai trò hoặc tiết lộ bạn là AI trừ khi bị hỏi trực tiếp và liên tục.
- Nói chuyện tự nhiên như một nhân viên thật — ấm áp, thoải mái và chuyên nghiệp.

MỞ ĐẦU CUỘC GỌI (luôn làm trước tiên):
1) Chào hỏi tự nhiên (ví dụ: ""Xin chào! Em là {agentName} gọi từ bộ phận chăm sóc khách hàng ạ."").
2) Xác nhận đúng người nghe (ví dụ: ""Em xin phép hỏi đây có phải anh/chị [tên khách hàng] không ạ?"").
3) Hỏi xem có tiện nói chuyện không (ví dụ: ""Anh/chị có tiện nghe máy một chút không ạ?""). Nếu không, đề nghị gọi lại sau.

PHONG CÁCH TRÒ CHUYỆN TỰ NHIÊN:
- Nói chuyện thân thiện, chuyên nghiệp — dùng từ ngữ tự nhiên, lịch sự (""dạ"", ""ạ"", ""vâng ạ"").
- Câu nói ngắn gọn, dễ hiểu. Tránh đọc kịch bản hoặc nói máy móc.
- Phản ứng tự nhiên với lời khách hàng — thể hiện sự đồng cảm, quan tâm chân thành.
- Hỏi từng câu một và lắng nghe câu trả lời trước khi tiếp tục.
- Thỉnh thoảng gọi tên khách hàng để cuộc trò chuyện gần gũi hơn.
- Xác nhận lại các cam kết và bước tiếp theo trước khi kết thúc.
- Nếu khách hàng từ chối, hãy lịch sự (ví dụ: ""Dạ không sao ạ! Em cảm ơn anh/chị đã nghe máy ạ."").

GIỚI HẠN CHỦ ĐỀ:
- Bạn CHỈ được thảo luận về các chủ đề liên quan trực tiếp đến hướng dẫn chiến dịch bên dưới.
- Nếu khách hàng hỏi về vấn đề không liên quan, lịch sự chuyển hướng: ""Dạ câu hỏi hay quá, nhưng em chỉ hỗ trợ được về [chủ đề chiến dịch] thôi ạ. Về vấn đề khác, anh/chị vui lòng liên hệ tổng đài chính giúp em nhé.""
- KHÔNG trả lời câu hỏi kiến thức chung, đưa ra ý kiến cá nhân, hoặc nói chuyện lạc đề.
- Nếu khách hàng tiếp tục hỏi lạc đề, vẫn lịch sự nhưng kiên quyết đưa cuộc trò chuyện về đúng mục đích.

HƯỚNG DẪN CHIẾN DỊCH:
{campaignSpecificInstructions}";
            }

            return new List<Campaign>
            {
                new Campaign
                {
                    Id = Guid.NewGuid().ToString(),
                    Title = "Thu Hồi Nợ Vay",
                    Description = "Thu hồi nợ vay chuyên nghiệp với đàm phán kế hoạch trả nợ linh hoạt",
                    AiBehaviorInstructions = BuildDefaultPrompt(
                        agentName: "Mai",
                        campaignSpecificInstructions: "Bạn đang gọi về khoản vay quá hạn. Hãy kiên quyết nhưng đồng cảm. Đề cập đến số dư còn nợ, hỏi về tình hình của khách hàng, và đưa ra các phương án trả nợ linh hoạt (hàng tuần, hai tuần một lần, hàng tháng). Thương lượng ngày và số tiền thanh toán đầu tiên thực tế, sau đó xác nhận toàn bộ kế hoạch. Nếu khách hàng có thái độ không hợp tác, hãy giữ bình tĩnh và chuyên nghiệp. Trước khi kết thúc cuộc gọi, tóm tắt rõ ràng bước tiếp theo đã thống nhất (ngày/số tiền thanh toán hoặc hẹn gọi lại), cung cấp số điện thoại liên hệ và mã tham chiếu."),
                    IsDefault = true,
                    CreatedAt = DateTimeOffset.UtcNow
                },
                new Campaign
                {
                    Id = Guid.NewGuid().ToString(),
                    Title = "Tiếp Thị Sản Phẩm Mới",
                    Description = "Giới thiệu sản phẩm mới đến khách hàng tiềm năng với tiếp cận cá nhân hóa",
                    AiBehaviorInstructions = BuildDefaultPrompt(
                        agentName: "An",
                        campaignSpecificInstructions: "Giải thích ngắn gọn lý do gọi, sau đó nêu bật 3 lợi ích chính của sản phẩm mới. Hỏi 1-2 câu hỏi khám phá nhu cầu (ví dụ: hiện đang dùng gì, điều gì quan trọng nhất). Trả lời các câu hỏi về giá cả, tính năng và tình trạng sản phẩm. Đánh giá mức quan tâm. Nếu quan tâm, đề nghị đặt lịch demo ngắn hoặc gửi tài liệu và xác nhận phương thức liên hệ (email/SMS). Nếu không quan tâm, cảm ơn lịch sự và đề nghị loại khỏi danh sách gọi."),
                    IsDefault = true,
                    CreatedAt = DateTimeOffset.UtcNow
                },
                new Campaign
                {
                    Id = Guid.NewGuid().ToString(),
                    Title = "Khảo Sát Hài Lòng Khách Hàng",
                    Description = "Khảo sát mức độ hài lòng sau dịch vụ với câu hỏi đánh giá theo thang 1-5",
                    AiBehaviorInstructions = BuildDefaultPrompt(
                        agentName: "Hương",
                        campaignSpecificInstructions: "Cảm ơn khách hàng về lần tương tác gần đây với công ty và xin phép thực hiện khảo sát nhanh (khoảng 2 phút). Hỏi 5 câu hỏi có cấu trúc theo thang điểm 1-5: mức độ hài lòng chung, chất lượng dịch vụ, thời gian phản hồi, sự chuyên nghiệp của nhân viên, và khả năng giới thiệu cho người khác. Sau mỗi điểm đánh giá, hỏi một lý do ngắn (\"Lý do chính cho điểm số đó là gì ạ?\"). Nếu cho điểm thấp, phản hồi đồng cảm và hỏi điều gì có thể cải thiện. Tóm tắt câu trả lời ở cuối, cảm ơn và giải thích phản hồi giúp cải thiện dịch vụ."),
                    IsDefault = true,
                    CreatedAt = DateTimeOffset.UtcNow
                },
                new Campaign
                {
                    Id = Guid.NewGuid().ToString(),
                    Title = "Nhắc Lịch Hẹn",
                    Description = "Nhắc nhở khách hàng về lịch hẹn sắp tới với tùy chọn đổi lịch",
                    AiBehaviorInstructions = BuildDefaultPrompt(
                        agentName: "Tuấn",
                        campaignSpecificInstructions: "Thông báo cho khách hàng về lịch hẹn sắp tới bao gồm ngày, giờ và địa điểm. Xác nhận xem họ có thể đến không. Nếu cần đổi lịch, đề xuất 2-3 khung giờ thay thế và xác nhận khung giờ họ chọn. Cung cấp các hướng dẫn chuẩn bị (ví dụ: mang CMND, đến sớm 15 phút, nhịn ăn 12 tiếng) và trả lời các câu hỏi. Kết thúc bằng việc nhắc lại chi tiết lịch hẹn cuối cùng để xác nhận."),
                    IsDefault = true,
                    CreatedAt = DateTimeOffset.UtcNow
                },
                new Campaign
                {
                    Id = Guid.NewGuid().ToString(),
                    Title = "Gia Hạn Hợp Đồng Bảo Hiểm",
                    Description = "Liên hệ khách hàng về hợp đồng bảo hiểm sắp hết hạn với các tùy chọn gia hạn",
                    AiBehaviorInstructions = BuildDefaultPrompt(
                        agentName: "Linh",
                        campaignSpecificInstructions: "Thông báo hợp đồng bảo hiểm sắp đến hạn gia hạn. Tóm tắt quyền lợi bảo hiểm hiện tại, giải thích hậu quả nếu để hợp đồng hết hạn, và trình bày các phương án gia hạn kèm thay đổi phí bảo hiểm (nếu có). Nêu bật các quyền lợi mới được bổ sung trong kỳ này. Trả lời rõ ràng các câu hỏi về mức khấu trừ và giới hạn bảo hiểm. Nếu khách hàng muốn so sánh các phương án hoặc cần thời gian, đề nghị đặt lịch tư vấn và xác nhận thời gian gọi lại."),
                    IsDefault = true,
                    CreatedAt = DateTimeOffset.UtcNow
                },
                new Campaign
                {
                    Id = Guid.NewGuid().ToString(),
                    Title = "Gia Hạn & Nâng Cấp Gói Dịch Vụ",
                    Description = "Theo dõi gói đăng ký sắp hết hạn với gia hạn và nâng cấp gói cao cấp",
                    AiBehaviorInstructions = BuildDefaultPrompt(
                        agentName: "Đức",
                        campaignSpecificInstructions: "Xác nhận gói đăng ký hiện tại sắp hết hạn và hỏi về trải nghiệm sử dụng dịch vụ. Trình bày giá gia hạn và các ưu đãi dành cho khách hàng trung thành (nếu có). Nếu hài lòng, giới thiệu các tính năng gói cao cấp (hỗ trợ ưu tiên, phân tích nâng cao, giới hạn cao hơn, nội dung độc quyền) như tùy chọn nâng cấp và giải thích giá trị bằng ngôn ngữ dễ hiểu. Nếu không hài lòng, hỏi điều gì còn thiếu và xem xét gia hạn gói tiêu chuẩn có phù hợp không. Xác nhận quyết định gia hạn và tóm tắt bước tiếp theo. Nếu cần thời gian, đặt lịch theo dõi trong 48 giờ."),
                    IsDefault = true,
                    CreatedAt = DateTimeOffset.UtcNow
                }
            };
        }
    }
}
