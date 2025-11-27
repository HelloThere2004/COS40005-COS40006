import json
import boto3
import os
import uuid
from datetime import datetime

dynamodb = boto3.resource('dynamodb')
TABLE_NAME = os.environ.get('TABLE_NAME')

def lambda_handler(event, context):
    table = dynamodb.Table(TABLE_NAME)
    
    # Input từ Bedrock Lambda
    article_id = f"ART#{event.get('article_id')}"
    review_json_str = event.get('review_result') # Giả sử Bedrock trả về JSON string
    
    try:
        # 1. Parse kết quả từ Bedrock (Cần nhắc Bedrock trả về đúng JSON format)
        # Ví dụ format mong đợi: [{"type": "Grammar", "original": "...", ...}]
        errors = json.loads(review_json_str) 
        
        # 2. Lưu từng lỗi (Batch Write để nhanh hơn)
        with table.batch_writer() as batch:
            for idx, err in enumerate(errors):
                batch.put_item(Item={
                    "PK": article_id,
                    "SK": f"ERR#{uuid.uuid4().hex[:6]}", # Hoặc dùng timestamp
                    "ChunkIndex": event.get('chunk_index'),
                    "ErrorType": err.get('type', 'General'),
                    "Severity": err.get('severity', 'LOW'),
                    "OriginalText": err.get('original'),
                    "CurrentTranslation": err.get('translation'),
                    "SuggestedFix": err.get('fix'),
                    "Explanation": err.get('explanation'),
                    "CreatedAt": datetime.now().isoformat()
                })

        # 3. Cập nhật (hoặc tạo) Metadata tổng cho bài viết
        # Chỗ này cần Atomic Counter để cộng dồn số lỗi nếu chạy song song nhiều chunk
        # Nhưng để đơn giản, ta có thể UpdateItem
        table.update_item(
            Key={'PK': article_id, 'SK': 'METADATA'},
            UpdateExpression="SET ErrorCount = if_not_exists(ErrorCount, :start) + :inc, LastUpdated = :now, Status = :status",
            ExpressionAttributeValues={
                ':start': 0,
                ':inc': len(errors),
                ':now': datetime.now().isoformat(),
                ':status': 'Pending'
            }
        )
        
        return {"status": "success", "saved_count": len(errors)}

    except Exception as e:
        print(f"Error saving: {e}")
        return {"status": "error", "message": str(e)}