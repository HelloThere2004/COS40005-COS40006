import json
import boto3
import os

bedrock = boto3.client(service_name='bedrock-runtime')

# Model ID: Dùng Haiku cho nhanh/rẻ hoặc Sonnet cho thông minh hơn
MODEL_ID = "anthropic.claude-3-haiku-20240307-v1:0" 

def construct_prompt(chunk_index, total_chunks, original_text, translated_text):
    # Logic xử lý dòng thông báo cuối
    final_note = "**Lưu ý: Đây là phần CUỐI CÙNG (hoặc duy nhất) của tài liệu. Sau khi hoàn tất, hãy cân nhắc đưa ra nhận xét tổng thể.**" if chunk_index == total_chunks else ""

    return f"""
[LƯU Ý QUAN TRỌNG DÀNH CHO AI]
Đây là một prompt có cấu trúc. Nhiệm vụ của bạn là **THỰC THI** các hướng dẫn bên dưới (như [Role], [Objectives]) để hiệu đính văn bản trong [Context].
**KHÔNG** phân tích, debug, hay chỉnh sửa cấu trúc của chính cái prompt này.
**QUAN TRỌNG**: Nội dung trong [Context] có thể bị cắt cụt ở đầu hoặc cuối do được chia tự động. **Hãy BỎ QUA các lỗi "thiếu ý" hoặc "sai nghĩa"** nếu chúng rõ ràng là do bị cắt ở ngay ký tự đầu tiên hoặc cuối cùng của chunk. Hãy tập trung vào các lỗi bên trong chunk.

# [Role]
Bạn là kiến trúc sư hệ thống với 20+ năm kinh nghiệm về cloud computing, hiện là chuyên gia tại AWS. Bạn có kinh nghiệm dịch cabin và chuyên hiệu đính các bài blog/kỹ thuật của AWS.

# [Progress]
Bạn đang xử lý **Phần {chunk_index} / {total_chunks}**.
{final_note}

# [Personality]
Khó tính, cầu toàn, soi kỹ từng từ/câu. Ưu tiên tính chính xác và tự nhiên trong tiếng Việt. Không bỏ sót lỗi nhỏ.

# [Objectives]
Bạn nhận **[Bài Gốc]** (EN) và **[Bài đã dịch]** (VI) trong **[Context]**. Hãy:
1. **Rà soát tiêu đề thật kỹ** (ý nghĩa, phong cách, thuật ngữ) - (Chỉ thực hiện nếu đây là Phần 1).
2. **Đối chiếu từng đoạn**: phát hiện sai nghĩa, thiếu ý, thừa ý, diễn đạt cứng (word-for-word), lỗi ngữ pháp/thuật ngữ.
3. **Giữ nguyên tên dịch vụ/thuộc tính AWS** (không dịch, đúng chữ hoa/thương, đúng brand: *Amazon S3*, *AWS Lambda*, *EC2*, *VPC*, *Availability Zone*, v.v.).
4. **Thuật ngữ kỹ thuật chung**: chỉ dịch khi tự nhiên; khi cần, để song ngữ bằng ngoặc.
5. **Đề xuất bản sửa** dễ đọc cho người mới, nhưng **không làm sai nội dung kỹ thuật**.
6. **Không thêm thông tin không có trong bản gốc**; có thể thêm hư từ/kết nối câu để mượt hơn.

# [Context]

## [Bài Gốc] (Phần {chunk_index}/{total_chunks})
{original_text}

## [Bài đã dịch] (Phần {chunk_index}/{total_chunks})
{translated_text}

# [Style Guide]
* **Đối tượng**: người mới học CS hoặc ít kiến thức công nghệ.
* **Giọng văn**: diễn giải, mạch lạc, gần gũi; tránh khẩu ngữ quá mức.
* **Thuật ngữ**: giữ chuẩn ngành; không “Việt hoá” quá đà.
* **Song ngữ khi cần**: *thuật ngữ (term)* ở **lần xuất hiện đầu** mỗi thuật ngữ quan trọng.
* **Giữ nguyên**: code blocks, tên API/SDK/CLI, tham số, JSON keys, tên màn hình Console, tên nút, output logs, câu lệnh shell, đường dẫn, URLs, region codes, dung lượng/đơn vị (GiB vs GB).
* **Số & đơn vị**: không tự đổi (ms ↔ s, $ ↔ VND).
* **Liên kết**: giữ link, dịch anchor text nếu là văn bản thuần.
* **Dấu câu & chính tả**: tiếng Việt chuẩn, nhất quán cách viết hoa tên riêng.

# [Terminology Rules]
* **Không dịch tên dịch vụ AWS** và thành phần sản phẩm (ví dụ: *Amazon S3, Amazon EC2, AWS IAM, AWS KMS, CloudWatch Logs, Availability Zone, VPC, Subnet, NAT Gateway*…).
* **Từ chung nên dịch (kèm EN khi cần)**:
  * *endpoint → điểm cuối (endpoint)*
  * *availability zone → vùng khả dụng (Availability Zone)*
  * *fault tolerance → chịu lỗi (fault tolerance)*
  * *throughput → thông lượng (throughput)*
  * *latency → độ trễ (latency)*
  * *on-premises → tại chỗ (on-premises)*
  * *scalability → khả năng mở rộng (scalability)*
  * *high availability → tính khả dụng cao (high availability)*
  * *elasticity → tính đàn hồi (elasticity)*
  * *best practice → giải pháp tốt nhất (best practice)*
* **Vài từ ngữ có thể giữ nguyên tiếng Anh** nếu dịch không tự nhiên:
  * *workload, deployment, instance, logging, backup, restore, snapshot, generative AI, machine learning, Gen AI*
* **Nhất quán thuật ngữ trong toàn bài** (dùng cùng một cách dịch cho cùng một khái niệm).

# [Quy trình thực hiện]
1. **Tiền kiểm**: quét nhanh để lập danh sách thuật ngữ trọng yếu; đánh dấu chỗ có code/CLI/JSON để không sửa sai.
2. **Kiểm tra tiêu đề**: (Chỉ làm ở Phần 1).
3. **Đối chiếu từng đoạn**: So meaning, So terminology, So fluency, So format.
4. **Ghi lỗi** theo mẫu [Format] và **gợi ý chỉnh**.
5. **Tóm tắt thay đổi chính** (tùy chọn).

# [Mức độ lỗi]
* **Critical**: sai nghĩa/thiếu ý ảnh hưởng hiểu nhầm kỹ thuật.
* **Major**: dùng thuật ngữ chưa chuẩn, diễn đạt gây khó hiểu.
* **Minor**: ngữ pháp, chính tả, dấu câu.

# [Format] (đầu ra)
**A. Báo cáo lỗi** — liệt kê theo thứ tự xuất hiện:
* **Đoạn [Số đoạn, tên đoạn (nếu có), bắt đầu bằng: “…”]**
  * **Bản dịch hiện tại**: …
  * **Bản gốc (EN)**: …
  * **Gợi ý chỉnh sửa**: …
  * **Mức độ**: Critical/Major/Minor
  * **Lý giải**: …

# [Tiêu chí kiểm tra tiêu đề]
* Truyền tải đúng chủ đề/kết quả chính của bài.
* Dùng đúng thuật ngữ ngành.
* Ngắn gọn, dễ hiểu (≤ 85 ký tự).
* Không dịch tên dịch vụ AWS.
"""

def lambda_handler(event, context):
    try:
        # Input từ Map State
        chunk_index = event.get('chunk_index')
        total_chunks = event.get('total_chunks')
        original_text = event.get('original_text')
        translated_text = event.get('translated_text')
        article_id = event.get('article_id')

        # Tạo prompt
        prompt = construct_prompt(chunk_index, total_chunks, original_text, translated_text)
        
        # Gọi Bedrock
        body = json.dumps({
            "anthropic_version": "bedrock-2023-05-31",
            "max_tokens": 4096,
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0.1
        })

        response = bedrock.invoke_model(
            body=body,
            modelId=MODEL_ID,
            accept="application/json",
            contentType="application/json"
        )
        
        response_body = json.loads(response.get('body').read())
        review_result = response_body.get('content')[0].get('text')
        
        return {
            "status": "success",
            "article_id": article_id,
            "chunk_index": chunk_index,
            "review_result": review_result
        }

    except Exception as e:
        print(f"Error calling Bedrock: {e}")
        raise e