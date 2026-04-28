from datetime import datetime
from typing import Any, Optional

from pydantic import BaseModel, ConfigDict, Field


class SourceEntry(BaseModel):
    source: str                           # "smpl_children", "smpl_jwatch", etc.
    internal_id: str                      # source-specific ID
    matching_method: Optional[str] = None # "by_imdbid", "by_similarity", "by_openai"
    mode: Optional[str] = None            # "by_provider", "by_search"


class ReleaseDate(BaseModel):
    day: Optional[int] = None
    month: Optional[int] = None
    year: Optional[int] = None


class UserReview(BaseModel):
    review_id: Optional[str] = None
    user_id: Optional[str] = None
    username: Optional[str] = None
    rating: Optional[float] = None
    summary: Optional[str] = None
    text: Optional[str] = None
    submission_date: Optional[str] = None
    up_votes: Optional[int] = None
    down_votes: Optional[int] = None
    spoiler: Optional[bool] = None


class CastMember(BaseModel):
    name_id: str                                      # IMDB "nameId" (nm...)
    name: str                                         # "actorNameText"
    character_name: Optional[str | list[str]] = None  # can be multi-role
    category: Optional[str | list[str]] = None        # actor, director, etc.
    attributes: Optional[list[str]] = None            # uncredited, voice, etc.
    episode_credits: Optional[int] = None
    position_ordered: Optional[str | int] = None
    profile_image_url: Optional[str] = None
    profile_image_path: Optional[str] = None          # local downloaded file


class Quote(BaseModel):
    quote_id: Optional[str] = None
    text: Optional[str] = None           # HTML body
    raw_text: Optional[str] = None       # plain text
    name_id: Optional[str] = None        # matched actor
    character_name: Optional[str] = None
    votes_interested: Optional[int] = None
    votes_total: Optional[int] = None


class CharacterImage(BaseModel):
    image_id: Optional[str] = None
    image_url: Optional[str] = None
    image_path: Optional[str] = None     # local downloaded file
    caption: Optional[str] = None
    actor_id: Optional[str] = None       # nameId of the actor


class Trailer(BaseModel):
    video_id: Optional[str] = None
    name: Optional[str] = None
    content_type: Optional[str] = None
    runtime: Optional[int] = None
    thumbnail_url: Optional[str] = None
    download_path: Optional[str] = None
    download_success: Optional[bool] = None
    downloaded_from: Optional[str] = None


class Subtitle(BaseModel):
    language: str
    subtitle_id: Optional[str] = None
    download_path: Optional[str] = None
    srt_path: Optional[str] = None
    parsed_path: Optional[str] = None
    download_success: Optional[bool] = None
    extract_success: Optional[bool] = None
    parse_success: Optional[bool] = None


class StreamingEntry(BaseModel):
    provider: str
    country: str


class PipelineMeta(BaseModel):
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    status: dict[str, str] = Field(default_factory=dict)


class Movie(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True, populate_by_name=True)

    # MongoDB _id
    id: Optional[Any] = Field(None, alias="_id", exclude=True)

    # === Identifiers ===
    imdb_id: Optional[str] = None
    parent_imdb_id: Optional[str] = None
    source_entries: list[SourceEntry] = Field(default_factory=list)

    # === Core Metadata ===
    title: Optional[str] = None
    original_title: Optional[str] = None
    year: Optional[int] = None
    release_date: Optional[ReleaseDate] = None
    runtime_seconds: Optional[int] = None
    genres: list[str] = Field(default_factory=list)
    certificate_rating: Optional[str] = None
    plot: Optional[str] = None
    synopsis: Optional[str] = None
    summary: Optional[str] = None
    plot_text: Optional[str] = None
    taglines: list[str] = Field(default_factory=list)
    keywords: list[str] = Field(default_factory=list)

    # === Ratings ===
    rating_value: Optional[float] = None
    rating_votes: Optional[int] = None
    metacritic_score: Optional[int] = None
    can_rate: Optional[bool] = None
    user_reviews: list[UserReview] = Field(default_factory=list)

    # === Primary Image ===
    primary_image_id: Optional[str] = None
    primary_image_url: Optional[str] = None

    # === Cast ===
    cast: list[CastMember] = Field(default_factory=list)

    # === Quotes (linked to cast via name_id) ===
    quotes: list[Quote] = Field(default_factory=list)

    # === Character Images ===
    character_images: list[CharacterImage] = Field(default_factory=list)

    # === Media ===
    trailers: list[Trailer] = Field(default_factory=list)
    subtitles: list[Subtitle] = Field(default_factory=list)

    # === Streaming ===
    streaming: list[StreamingEntry] = Field(default_factory=list)

    # === Overview Counts ===
    total_images: Optional[int] = None
    total_videos: Optional[int] = None
    total_taglines: Optional[int] = None
    total_plots: Optional[int] = None
    total_keywords: Optional[int] = None
    total_quotes: Optional[int] = None

    # === Pipeline Meta ===
    meta: PipelineMeta = Field(default_factory=PipelineMeta, alias="_meta")
