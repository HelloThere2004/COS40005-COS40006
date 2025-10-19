import argparse
import json
import boto3
import pandas as pd
from botocore.exceptions import ClientError

# --- CẤU HÌNH ---
# Dán URL của SQS Queue bạn đã tạo ở Bước 1 vào đây
SQS_QUEUE_URL = 'https://sqs.us-east-1.amazonaws.com/211125563303/process_extract_url'
# Tên các cột trong file CSV/Excel của bạn
COLUMN_AWS_URL = 'aws_blog_url'
COLUMN_GDOC_URL = 'google_doc_url'

def send_to_sqs(sqs_client, queue_url, aws_url, gdoc_url):
    """
    Gửi một message chứa hai URL vào hàng đợi SQS.
    Message được gửi dưới dạng chuỗi JSON.
    """
    message_body = {
        'aws_blog_url': aws_url,
        'google_doc_url': gdoc_url
    }
    
    try:
        response = sqs_client.send_message(
            QueueUrl=queue_url,
            MessageBody=json.dumps(message_body)
        )
        return response['MessageId']
    except ClientError as e:
        print(f"Lỗi khi gửi message đến SQS: {e}")
        return None

def process_file(file_path, queue_url):
    """
    Đọc file CSV hoặc Excel, lặp qua từng dòng và gửi dữ liệu đến SQS.
    """
    if not file_path:
        print("Lỗi: Vui lòng cung cấp đường dẫn đến file.")
        return

    print(f"Đang xử lý file: {file_path}")

    try:
        # Sử dụng pandas để đọc được cả CSV và Excel một cách linh hoạt
        if file_path.endswith('.csv'):
            df = pd.read_csv(file_path)
        elif file_path.endswith(('.xls', '.xlsx')):
            df = pd.read_excel(file_path)
        else:
            print("Lỗi: Định dạng file không được hỗ trợ. Vui lòng sử dụng .csv hoặc .xlsx.")
            return
    except FileNotFoundError:
        print(f"Lỗi: Không tìm thấy file tại '{file_path}'")
        return
    except Exception as e:
        print(f"Lỗi khi đọc file: {e}")
        return

    # Kiểm tra xem các cột cần thiết có tồn tại không
    if COLUMN_AWS_URL not in df.columns or COLUMN_GDOC_URL not in df.columns:
        print(f"Lỗi: File phải chứa 2 cột tên là '{COLUMN_AWS_URL}' và '{COLUMN_GDOC_URL}'.")
        return

    # Khởi tạo SQS client
    # Boto3 sẽ tự động tìm credentials từ môi trường của bạn (ví dụ: từ `aws configure`)
    sqs_client = boto3.client('sqs')
    
    total_rows = len(df)
    success_count = 0
    
    print(f"Tìm thấy {total_rows} dòng. Bắt đầu gửi đến SQS...")

    # Lặp qua từng dòng trong DataFrame
    for index, row in df.iterrows():
        aws_url = row[COLUMN_AWS_URL]
        gdoc_url = row[COLUMN_GDOC_URL]

        # Kiểm tra dữ liệu URL có hợp lệ không (không rỗng)
        if pd.isna(aws_url) or pd.isna(gdoc_url):
            print(f"Cảnh báo: Bỏ qua dòng {index + 1} vì thiếu URL.")
            continue
            
        message_id = send_to_sqs(sqs_client, queue_url, aws_url, gdoc_url)
        
        if message_id:
            print(f"Đã gửi thành công dòng {index + 1}/{total_rows}. Message ID: {message_id}")
            success_count += 1
        else:
            print(f"Gửi thất bại dòng {index + 1}. Vui lòng kiểm tra lại cấu hình AWS và URL của SQS.")

    print("\n--- HOÀN TẤT ---")
    print(f"Tổng số dòng đã gửi thành công: {success_count}/{total_rows}")

if __name__ == '__main__':
    # Kiểm tra xem SQS_QUEUE_URL đã được cấu hình chưa
    if 'YOUR_SQS_QUEUE_URL' in SQS_QUEUE_URL:
        print("VUI LÒNG CẤU HÌNH BIẾN 'SQS_QUEUE_URL' TRONG SCRIPT TRƯỚC KHI CHẠY.")
    else:
        # Thiết lập parser để nhận tham số từ dòng lệnh
        parser = argparse.ArgumentParser(
            description='Đọc file CSV/Excel chứa các URL và gửi chúng vào một hàng đợi AWS SQS.'
        )
        parser.add_argument(
            'file_path', 
            type=str, 
            help='Đường dẫn đến file .csv hoặc .xlsx cần xử lý.'
        )
        
        args = parser.parse_args()
        
        process_file(args.file_path, SQS_QUEUE_URL)
