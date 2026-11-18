"""Настройки процесса без секретов в исходном коде."""

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    pg_host: str = "localhost"
    pg_port: int = 5432
    pg_database: str = "reels_router"
    pg_user: str = "reels_router"
    pg_password: str = ""
    pg_sslmode: str = "require"
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_security_protocol: str = "PLAINTEXT"
    kafka_sasl_mechanism: str = "PLAIN"
    kafka_sasl_username: str = ""
    kafka_sasl_password: str = ""
    kafka_ssl_ca_file: str = ""
    kafka_topic_media: str = "reels.media.commands.v1"
    kafka_topic_events: str = "reels.pipeline.events.v1"
    kafka_group_media: str = "reels-media"
    kafka_group_coordinator: str = "reels-coordinator"
    kafka_group_frames: str = "reels-frames"
    kafka_topic_frames: str = "reels.frames.commands.v1"
    kafka_topic_asr: str = "reels.asr.commands.v1"
    kafka_group_asr: str = "reels-asr"
    kafka_topic_vlm: str = "reels.vlm.commands.v1"
    kafka_group_vlm: str = "reels-vlm"
    kafka_topic_llm: str = "reels.llm.commands.v1"
    kafka_group_llm: str = "reels-llm"
    kafka_topic_geo: str = "reels.geo.commands.v1"
    kafka_group_geo: str = "reels-geo"
    geoapify_api_key: str = ""
    geoapify_base_url: str = "https://api.geoapify.com"
    internal_api_token: str = ""
    s3_endpoint_url: str = "http://localhost:9000"
    s3_region: str = "us-east-1"
    s3_access_key_id: str = ""
    s3_secret_access_key: str = ""
    s3_bucket: str = "reels-router"
    scratch_dir: str = "/tmp"
    max_video_seconds: int = 180
    max_download_bytes: int = 104857600
    stage_timeout_seconds: int = 600
    instagram_cookies_file: str = ""
    gemma_model: str = "google/embeddinggemma-2"
    gemma_cpu_threads: int = 3
    frame_top_k: int = 10
    asr_provider: str = ""
    asr_base_url: str = ""
    asr_api_key: str = ""
    asr_model: str = ""
    asr_max_bytes: int = 25000000
    llm_provider: str = ""
    llm_base_url: str = ""
    llm_api_key: str = ""
    llm_model: str = ""
    vlm_provider: str = ""
    vlm_base_url: str = ""
    vlm_api_key: str = ""
    vlm_model: str = ""
