import os
import datetime
import argparse
from dotenv import load_dotenv
from ks3.connection import Connection
from tqdm import tqdm

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
            timeout=1
        )
        self.bucket_name = os.getenv("KS3_BUCKET")
        self.bucket = self.conn.get_bucket(self.bucket_name)

    def _validate_config(self):
        """检查环境变量配置"""
        required_vars = ["KS3_ACCESS_KEY", "KS3_SECRET_KEY", "KS3_ENDPOINT", "KS3_BUCKET"]
        missing = [var for var in required_vars if not os.getenv(var)]
        if missing:
            raise ValueError(f"缺少以下环境变量配置: {', '.join(missing)}")

    def upload_file(self, local_path, remote_key):
        """执行上传操作"""
        if not os.path.exists(local_path):
            raise FileNotFoundError(f"本地文件不存在: {local_path}")

        file_size = os.path.getsize(local_path)
        print(f"开始上传至 KS3 存储桶 [{self.bucket_name}]:")
        print(f"本地路径:  {os.path.abspath(local_path)}")
        print(f"远程路径: {remote_key} ({file_size/1024/1024:.2f} MB)")

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

    def generate_presigned_url(self, object_key, expires_in_seconds=3600):
        """
        生成预签名下载链接（默认有效期 1 小时）
        """
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
    parser = argparse.ArgumentParser(
        description="上传文件至金山云 KS3",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "local_file",
        help="待上传的本地文件路径"
    )
    parser.add_argument(
        "ks3_path",
        help="KS3 中的目标路径（例如 'backups/test.zip'）"
    )
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
