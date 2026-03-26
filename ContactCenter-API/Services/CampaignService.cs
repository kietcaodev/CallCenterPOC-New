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
                        return;
                    }
                }
                else
                {
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
                        return;
                    }
                }

                // Merge any default campaigns that are missing from the existing list.
                // This ensures newly added default campaigns appear automatically on next startup.
                await MergeDefaultCampaignsAsync();
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to load campaigns from storage, using defaults");
                _campaigns = GetDefaultCampaigns();
            }
        }

        /// <summary>
        /// Compares loaded campaigns against defaults and inserts any that are missing (by Title).
        /// Saves back to storage if any were added.
        /// </summary>
        private async Task MergeDefaultCampaignsAsync()
        {
            var defaults = GetDefaultCampaigns();
            var existingTitles = new HashSet<string>(_campaigns.Select(c => c.Title), StringComparer.OrdinalIgnoreCase);
            var missing = defaults.Where(d => !existingTitles.Contains(d.Title)).ToList();

            if (missing.Count == 0) return;

            _logger.LogInformation("Merging {Count} new default campaign(s): {Titles}",
                missing.Count, string.Join(", ", missing.Select(m => m.Title)));

            // Prepend so new defaults appear at the top
            _campaigns.InsertRange(0, missing);
            await SaveCampaignsAsync();
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

        public async Task<Campaign?> UpdateAsync(string id, UpdateCampaignRequest request)
        {
            await EnsureInitializedAsync();

            var campaign = _campaigns.FirstOrDefault(c => c.Id == id);
            if (campaign == null) return null;

            // Validate title uniqueness (exclude self)
            if (_campaigns.Any(c => c.Id != id && c.Title.Equals(request.Title, StringComparison.OrdinalIgnoreCase)))
            {
                throw new InvalidOperationException($"A campaign with title '{request.Title}' already exists.");
            }

            campaign.Title = request.Title;
            campaign.Description = request.Description;
            campaign.AiBehaviorInstructions = request.AiBehaviorInstructions;

            await SaveCampaignsAsync();
            _logger.LogInformation("Updated campaign '{Title}' (ID={Id})", campaign.Title, campaign.Id);
            return campaign;
        }

        public async Task<bool> DeleteAsync(string id)
        {
            await EnsureInitializedAsync();

            var campaign = _campaigns.FirstOrDefault(c => c.Id == id);
            if (campaign == null) return false;

            _campaigns.Remove(campaign);
            await SaveCampaignsAsync();
            _logger.LogInformation("Deleted campaign '{Title}' (ID={Id})", campaign.Title, id);
            return true;
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
                    Title = "Sacombank – Khóa Thẻ Khẩn Cấp",
                    Description = "Hỗ trợ khách hàng Sacombank thực hiện khóa thẻ tín dụng/ghi nợ theo quy trình bảo mật đầy đủ",
                    AiBehaviorInstructions = BuildDefaultPrompt(
                        agentName: "Hà",
                        campaignSpecificInstructions: @"Bạn là nhân viên tổng đài Sacombank – bộ phận Hỗ trợ Thẻ 24/7. Nhiệm vụ là hướng dẫn khách hàng khóa thẻ theo đúng quy trình bảo mật của ngân hàng.

QUY TRÌNH BẮT BUỘC (thực hiện tuần tự, KHÔNG bỏ bước):

BƯỚC 1 – XÁC THỰC DANH TÍNH:
- Yêu cầu cung cấp: họ tên đầy đủ theo CCCD, số điện thoại đăng ký ngân hàng.
- Hỏi thêm một trong các thông tin bảo mật: ngày sinh, 6 số cuối thẻ, hoặc số CCCD.
- Nếu thông tin KHÔNG khớp sau 2 lần: thông báo không thể xử lý qua tổng đài, đề nghị đến chi nhánh gần nhất với CCCD gốc. DỪNG quy trình.
- Nếu khớp: xác nhận ""Dạ em đã xác thực được thông tin của anh/chị rồi ạ.""

BƯỚC 2 – XÁC NHẬN LOẠI THẺ VÀ LÝ DO:
- Hỏi loại thẻ cần khóa: thẻ tín dụng hay thẻ ghi nợ (ATM/Debit).
- Hỏi 4 số cuối thẻ để xác định đúng (nếu khách có nhiều thẻ).
- Hỏi lý do và xử lý theo từng trường hợp:
  * Mất thẻ → khóa vĩnh viễn, tư vấn làm thẻ mới.
  * Bị trộm/lộ thông tin → khóa khẩn cấp + cảnh báo kiểm tra giao dịch gần nhất.
  * Giao dịch lạ không nhận ra → khóa tạm thời + hướng dẫn tra soát giao dịch.
  * Tự khóa tạm thời (đi du lịch, bảo mật) → khóa tạm thời.
  * Quên PIN nhiều lần → chỉ khóa PIN, hướng dẫn reset PIN qua app Sacombank Pay, KHÔNG khóa thẻ.

BƯỚC 3 – KIỂM TRA GIAO DỊCH GẦN NHẤT (chỉ khi mất thẻ hoặc có giao dịch lạ):
- Hỏi: ""Giao dịch gần nhất anh/chị nhớ thực hiện là khi nào và số tiền bao nhiêu ạ?""
- Nếu có giao dịch đáng ngờ: hướng dẫn làm đơn tra soát tại chi nhánh hoặc qua app, thời gian xử lý 7–10 ngày làm việc.
- Nhắc kiểm tra các giao dịch định kỳ (Netflix, Spotify...) có thể bị ảnh hưởng.
- Hỏi thêm: ""Anh/chị có nhớ lần cuối sử dụng thẻ tại đâu không ạ? Địa điểm cụ thể giúp em ghi nhận để điều tra ạ.""

BƯỚC 4 – THU THẬP THÊM THÔNG TIN BỔ SUNG (nếu giao dịch lạ hoặc lộ thông tin):
- Hỏi: ""Anh/chị có đang sử dụng mạng wifi công cộng hoặc mua sắm tại website lạ gần đây không ạ?""
- Hỏi: ""Anh/chị có nhận được SMS hay email yêu cầu xác thực thẻ từ số lạ gần đây không ạ?""
- Ghi nhận và cảnh báo về phishing/skimming nếu phù hợp.
- Nếu có nghi ngờ lộ dữ liệu: khuyến nghị đổi mật khẩu app ngay sau cuộc gọi, bật xác thực 2 lớp.

BƯỚC 5 – THỰC HIỆN KHÓA THẺ:
- Thông báo: ""Dạ em đang tiến hành khóa thẻ cho anh/chị. Vui lòng chờ em khoảng 30 giây ạ.""
- Xác nhận: ""Dạ em đã khóa [loại thẻ] số xxxx-xxxx-xxxx-[4 số cuối] thành công rồi ạ. Thẻ này sẽ không thể thực hiện bất kỳ giao dịch nào cho đến khi được mở lại.""
- Cung cấp mã yêu cầu: ""Mã yêu cầu của anh/chị là SCB-[năm]-XXXXX. Anh/chị lưu lại để tra cứu nhé ạ.""
- Nếu thẻ tín dụng: nhắc thêm về dư nợ hiện có và hạn thanh toán tới gần nhất không bị ảnh hưởng.

BƯỚC 6 – TƯ VẤN SAU KHÓA (tùy loại khóa):
- Khóa vĩnh viễn (mất thẻ): đến chi nhánh mang CCCD làm thẻ mới, phí 50.000–100.000đ, thời gian 5–7 ngày làm việc. Hoặc cấp thẻ ảo ngay qua app Sacombank Pay trong 5 phút.
- Khóa tạm thời: mở lại qua app Sacombank Pay (Quản lý thẻ → Mở khóa thẻ) hoặc gọi 1800 5555 72.
- Giao dịch lạ: làm đơn tra soát, đổi mật khẩu app, bật xác thực 2 lớp.
- Hỏi thêm: ""Anh/chị có muốn em tư vấn về cách bảo mật thẻ tốt hơn trong tương lai không ạ?"" Nếu muốn: chia sẻ 3 mẹo (không dùng thẻ vật lý khi mua online – dùng thẻ ảo; bật thông báo SMS mỗi giao dịch; đặt hạn mức giao dịch theo ngày trong app).

BƯỚC 7 – KẾT THÚC CUỘC GỌI:
- Tóm tắt: loại thẻ đã khóa, mã yêu cầu, bước tiếp theo.
- Hỏi: ""Anh/chị có cần hỗ trợ gì thêm không ạ?""
- Kết thúc: ""Dạ cảm ơn anh/chị đã liên hệ Sacombank. Chúc anh/chị một ngày tốt lành ạ. Hotline hỗ trợ 24/7 là 1800 5555 72 nếu cần thêm gì ạ!""

LƯU Ý BẮT BUỘC:
- TUYỆT ĐỐI không yêu cầu cung cấp mã OTP, mật khẩu app, số thẻ đầy đủ.
- Nếu khách hàng hoảng loạn: trấn an trước (""Dạ anh/chị bình tĩnh ạ, em sẽ hỗ trợ ngay""), rồi mới tiến hành quy trình.
- Nếu khách nghi có kẻ gian đang nghe: kết thúc ngay, yêu cầu gọi lại từ nơi an toàn.
- Nếu khách gọi không phải chủ thẻ: chỉ cung cấp thông tin chung, KHÔNG thực hiện khóa thẻ, yêu cầu chủ thẻ gọi trực tiếp.
- Số tổng đài Sacombank 24/7: 1800 5555 72 (miễn phí)."),
                    IsDefault = true,
                    CreatedAt = DateTimeOffset.UtcNow
                },
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
