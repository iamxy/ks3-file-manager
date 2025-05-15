import os
import io
import json
import datetime
import argparse
from dotenv import load_dotenv
from ks3.connection import Connection
from tqdm import tqdm
import time
from ks3.multipart import Part

class KS3Uploader:
    def __init__(self):
        load_dotenv()
        self._validate_config()
        self.conn = Connection(
            os.getenv("KS3_ACCESS_KEY"),
            os.getenv("KS3_SECRET_KEY"),
            host=os.getenv("KS3_ENDPOINT"),
            is_secure=os.getenv("KS3_USE_HTTPS", "True").lower() == "true",
            domain_mode=False,
            timeout=30
        )
        self.bucket_name = os.getenv("KS3_BUCKET")
        self.bucket = self.conn.get_bucket(self.bucket_name)

    def _validate_config(self):
        required_vars = ["KS3_ACCESS_KEY", "KS3_SECRET_KEY", "KS3_ENDPOINT", "KS3_BUCKET"]
        missing = [var for var in required_vars if not os.getenv(var)]
        if missing:
            raise ValueError(f"缺少以下环境变量配置: {', '.join(missing)}")

    def upload_file(self, local_path, remote_key):
        if not os.path.exists(local_path):
            raise FileNotFoundError(f"本地文件不存在: {local_path}")

        file_size = os.path.getsize(local_path)
        print(f"开始上传至 KS3 存储桶 [{self.bucket_name}]：")
        print(f"本地路径:  {os.path.abspath(local_path)}")
        print(f"远程路径: {remote_key} ({file_size/1024/1024:.2f} MB)")

        if file_size > 100 * 1024 * 1024:
            return self.multipart_upload_with_resume(local_path, remote_key, file_size)
        else:
            with tqdm(total=file_size, unit='B', unit_scale=True, desc="上传进度") as pbar:
                def callback(uploaded_bytes):
                    pbar.update(uploaded_bytes - pbar.n)

                try:
                    key = self.bucket.new_key(remote_key)
                    key.set_contents_from_filename(local_path, cb=callback)
                    print("\n✅ 上传成功！")
                    return f"ks3://{self.bucket_name}/{remote_key}"
                except Exception as e:
                    print(f"\n❌ 上传失败: {str(e)}")
                    return None

    def multipart_upload_with_resume(self, local_path, remote_key, file_size):
        part_size = 50 * 1024 * 1024
        part_count = (file_size + part_size - 1) // part_size
        resume_path = f".resume_{os.path.basename(local_path)}.json"

        if os.path.exists(resume_path):
            with open(resume_path, 'r') as f:
                resume_data = json.load(f)
                upload_id = resume_data['upload_id']
                uploaded_parts = {int(p['part_num']): p for p in resume_data['parts']}
                print("🔄 检测到断点续传记录，尝试恢复...")
        else:
            mp = self.bucket.initiate_multipart_upload(remote_key)
            upload_id = mp.id
            uploaded_parts = {}
            resume_data = {"upload_id": upload_id, "parts": []}
            with open(resume_path, 'w') as f:
                json.dump(resume_data, f)

        mp = self.bucket.get_all_multipart_uploads(prefix=remote_key)
        mp = next((x for x in mp if x.id == upload_id), None)

        if not mp:
            print("❌ 无法找到上传任务，终止续传。")
            return None

        parts = list(uploaded_parts.values())

        with open(local_path, 'rb') as f, tqdm(total=file_size, unit='B', unit_scale=True, desc="分片上传进度") as pbar:
            pbar.update(sum([p['size'] for p in parts]))
            for i in range(part_count):
                part_num = i + 1
                if part_num in uploaded_parts:
                    continue

                offset = i * part_size
                f.seek(offset)
                data = f.read(min(part_size, file_size - offset))

                for attempt in range(3):
                    try:
                        part = mp.upload_part_from_file(io.BytesIO(data), part_num=part_num)
                        etag = part.headers.get('ETag', '').strip('"')
                        parts.append({
                            "part_num": part_num,
                            "etag": etag,
                            "size": len(data)
                        })
                        resume_data['parts'].append({
                            "part_num": part_num,
                            "etag": etag,
                            "size": len(data)
                        })
                        with open(resume_path, 'w') as f_resume:
                            json.dump(resume_data, f_resume)
                        pbar.update(len(data))
                        break
                    except Exception as e:
                        print(f"⚠️ 分片 {part_num} 上传失败，重试 {attempt + 1}/3: {e}")
                        time.sleep(2 ** attempt)
                else:
                    print(f"❌ 分片 {part_num} 连续重试失败，取消上传任务。")
                    mp.cancel_upload()
                    return None

        mp.complete_upload()

        os.remove(resume_path)
        print("\n✅ 分片上传成功！")
        return f"ks3://{self.bucket_name}/{remote_key}"

    def generate_presigned_url(self, object_key, expires_in_seconds=3600):
        try:
            key = self.bucket.get_key(object_key)
            if not key:
                print(f"❌ 文件未找到: {object_key}")
                return None
            url = key.generate_url(expires_in_seconds, method='GET')
            return url
        except Exception as e:
            print(f"❌ 生成预签名链接失败: {str(e)}")
            return None

def main():
    parser = argparse.ArgumentParser(description="上传文件至金山云 KS3", formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("local_file", help="待上传的本地文件路径")
    parser.add_argument("ks3_path", help="KS3 中的目标路径（例如 'backups/test.zip'）")
    args = parser.parse_args()

    try:
        uploader = KS3Uploader()
        result = uploader.upload_file(args.local_file, args.ks3_path)
        if result:
            print("KS3 URI:", result)
            url = uploader.generate_presigned_url(args.ks3_path)
            if url:
                print("✅ 临时下载链接（1小时有效）:")
                print(url)
    except Exception as e:
        print(f"错误: {str(e)}")

if __name__ == "__main__":
    main()

