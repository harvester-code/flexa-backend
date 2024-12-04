import io

from fastapi import File, HTTPException, UploadFile, status
from PIL import Image, ImageOps
from supabase import Client

from src.database import supabase_public_clinet, aws_s3_client
from src.profiles.schema import InUserInfo

supabase: Client = supabase_public_clinet()
s3_client = aws_s3_client()


class ProfilesService:

    def fetch_user_info(self, user_id):

        response = (
            supabase.schema("users")
            .table("user_info")
            .select("first_name, last_name, email, profile_image_url, position, bio")
            .eq("user_id", user_id)
            .execute()
        )

        return {"message": {"status": 200, "result": response.data}}

    def update_user_info(self, item: InUserInfo):

        update_data = {
            "first_name": item.first_name,
            "last_name": item.last_name,
        }

        if item.position:
            update_data["position"] = item.position

        if item.bio:
            update_data["bio"] = item.bio

        if item.profile_image_url:
            update_data["profile_image_url"] = item.profile_image_url

        response = (
            supabase.schema("users")
            .table("user_info")
            .update(update_data)
            .eq("user_id", item.user_id)
            .execute()
        )

        return {"message": {"status": 201, "result": response.data}}

    def fetch_user_login_history(self, user_id):

        response = (
            supabase.schema("users")
            .table("user_login_history")
            .select("user_agent, ip_address, updated_at")
            .limit(5)
            .eq("user_id", user_id)
            .execute()
        )

        return {"message": {"status": 200, "result": response.data}}

    async def upload_image(self, user_id: str, file: UploadFile | None = File(None)):
        """
        이미지 업로드 테스트
        - 1. 클라이언트에서 서버로 이미지를 업로드한다.
        - 2. 이미지 확장자가 업로드 가능한지 확인한다.
        - 3. 이미지 사이즈가 업로드 가능한 크기인지 확인한다.
        - 4. 이미지 이름을 변경한다.
        - 5. 이미지를 최적화하여 저장한다.
        """
        if not file:
            return {"detail": "이미지 없음"}

        file = await self.validate_image_type(file)
        file = await self.validate_image_size(file)

        file = self.change_filename(file, user_id)
        filename = file.filename

        image = self.resize_image(file)
        image = self.convert_image_to_bytes(image)
        self.upload_to_s3(image, "flexa-prod-ap-northeast-2-data-storage", filename)

        response = {
            "profile_image_url": f"s3://flexa-prod-ap-northeast-2-data-storage/{filename}"
        }
        return {"message": {"status": 201, "result": response}}

    async def validate_image_type(self, file: UploadFile) -> UploadFile:
        if file.filename.split(".")[-1].lower() not in ["jpg", "jpeg", "png"]:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="업로드 불가능한 이미지 확장자입니다.",
            )

        if not file.content_type.startswith("image"):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="이미지 파일만 업로드 가능합니다.",
            )
        return file

    async def validate_image_size(self, file: UploadFile) -> UploadFile:
        if len(await file.read()) > 10 * 1024 * 1024:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="이미지 파일은 10MB 이하만 업로드 가능합니다.",
            )
        return file

    def change_filename(self, file: UploadFile, user_id) -> UploadFile:
        """
        이미지 이름 변경
        """

        file.filename = f"profiles/{user_id}.jpeg"
        return file

    def resize_image(self, file: UploadFile, max_size: int = 1024):
        read_image = Image.open(file.file)
        original_width, original_height = read_image.size

        if original_width > max_size or original_height > max_size:
            if original_width > original_height:
                new_width = max_size
                new_height = int((new_width / original_width) * original_height)
            else:
                new_height = max_size
                new_width = int((new_height / original_height) * original_width)
            read_image = read_image.resize((new_width, new_height))

        read_image = read_image.convert("RGB")
        read_image = ImageOps.exif_transpose(read_image)
        return read_image

    def convert_image_to_bytes(self, image: Image) -> io.BytesIO:
        img_byte = io.BytesIO()
        image.save(img_byte, "jpeg", quality=70)
        img_byte.seek(0)
        return img_byte

    def upload_to_s3(self, file: io.BytesIO, bucket_name: str, file_name: str) -> None:

        s3_client.upload_fileobj(
            file,
            bucket_name,
            file_name,
            ExtraArgs={"ContentType": "image/jpeg"},
        )
