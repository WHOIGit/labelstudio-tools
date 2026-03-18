from abc import ABC, abstractmethod
from dataclasses import dataclass


class BBox:
    """Bounding box in Label Studio percentage units (0-100).

    x, y are the top-left corner. width, height are dimensions.
    All values are percentages of the full image.
    """

    def __init__(self, x: float, y: float, width: float, height: float, rotation: float = 0.0):
        self.x = x
        self.y = y
        self.width = width
        self.height = height
        self.rotation = rotation

    def as_result_value(self) -> dict:
        """Return dict suitable for use in a Label Studio result value."""
        return self.to_dict()

    @classmethod
    def from_yolo(cls, x_center: float, y_center: float, width: float, height: float) -> 'BBox':
        """Create from YOLO format (center-based, normalized 0-1)."""
        x = (x_center - width / 2) * 100
        y = (y_center - height / 2) * 100
        return cls(x, y, width * 100, height * 100)

    @classmethod
    def from_pixels(cls, x_px: float, y_px: float, w_px: float, h_px: float,
                    img_w: int, img_h: int) -> 'BBox':
        """Create from pixel coordinates given image dimensions."""
        return cls(x_px / img_w * 100, y_px / img_h * 100,
                   w_px / img_w * 100, h_px / img_h * 100)

    def to_pixels(self, img_w: int, img_h: int) -> tuple:
        """Convert to pixel coordinates (x, y, w, h) given image dimensions."""
        return (self.x / 100 * img_w, self.y / 100 * img_h,
                self.width / 100 * img_w, self.height / 100 * img_h)

    def to_dict(self) -> dict:
        d = {'x': self.x, 'y': self.y, 'width': self.width, 'height': self.height}
        if self.rotation:
            d['rotation'] = self.rotation
        return d

    @classmethod
    def from_dict(cls, d: dict) -> 'BBox':
        return cls(d['x'], d['y'], d['width'], d['height'], d.get('rotation', 0.0))

    def __repr__(self):
        return f"BBox(x={self.x}, y={self.y}, width={self.width}, height={self.height}, rotation={self.rotation})"


@dataclass
class ResultField:
    """Describes one Label Studio result entry type.

    A single region (e.g., a bounding box) can generate multiple result entries
    sharing the same region_id -- the bbox itself, plus classification choices,
    plus notes, etc. ResultField helps build each entry without hardcoding
    from_name/to_name/type.
    """
    from_name: str
    to_name: str
    result_type: str  # 'rectangle', 'taxonomy', 'choices', 'textarea', etc.

    def build(self, value: dict, region_id: str = None, score: float = None,
              original_width: int = None, original_height: int = None) -> dict:
        """Build a complete Label Studio result dict."""
        result = {
            "type": self.result_type,
            "from_name": self.from_name,
            "to_name": self.to_name,
            "value": value,
        }
        if region_id is not None:
            result["id"] = region_id
        if score is not None:
            result["score"] = score
        if original_width is not None:
            result["original_width"] = original_width
        if original_height is not None:
            result["original_height"] = original_height
        return result


class BaseRegion(ABC):
    """Abstract base for a Label Studio region (e.g., a bounding box with metadata).

    A single region may produce multiple result entries (e.g., bbox + taxonomy +
    per-region choices), all sharing the same region_id.
    """

    def __init__(self, score: float = None, original_width: int = None,
                 original_height: int = None):
        self.score = score
        self.original_width = original_width
        self.original_height = original_height

    @abstractmethod
    def as_result_dicts(self, region_id: str = None) -> list:
        """Return list of LS result dicts for this region."""
        ...

    @abstractmethod
    def to_dict(self) -> dict:
        ...

    @classmethod
    @abstractmethod
    def from_dict(cls, d: dict) -> 'BaseRegion':
        ...


class BaseAnnotation(ABC):
    """Abstract base for a Label Studio annotation or prediction.

    Contains a list of regions and optional annotation-level metadata.
    """

    def __init__(self, regions: list = None, score: float = None,
                 model_version: str = None, original_width: int = None,
                 original_height: int = None):
        self.regions: list = regions or []
        self.score = score
        self.model_version = model_version
        self.original_width = original_width
        self.original_height = original_height

    def is_prediction(self) -> bool:
        """True if this has model_version or score."""
        return self.model_version is not None or self.score is not None

    def score_from_regions(self, method: str = 'mean'):
        """Compute annotation-level score from region scores."""
        scores = [r.score for r in self.regions if r.score is not None]
        if not scores:
            return None
        if method == 'mean':
            return sum(scores) / len(scores)
        elif method == 'min':
            return min(scores)
        elif method == 'max':
            return max(scores)
        raise ValueError(f"Unknown method: {method}")

    def as_dict(self, task_id: int = None, force: str = None) -> dict:
        """Convert to LS annotation/prediction dict format.

        Subclasses should override to prepend frame-level results before
        calling super().

        force: 'annotation' or 'prediction' to override auto-detection.
        """
        results = []
        for i, region in enumerate(self.regions):
            region_id = f"region_{i:03d}"
            if self.original_width and self.original_height:
                region.original_width = self.original_width
                region.original_height = self.original_height
            results.extend(region.as_result_dicts(region_id))

        output = {"result": results}

        average_score = self.score_from_regions()
        score = self.score or average_score

        if force == 'prediction':
            output['score'] = score or 0.50
            output['model_version'] = self.model_version or 'dummy-model-version'
        elif force != 'annotation':
            if score is not None:
                output['score'] = score
            if self.model_version is not None:
                output['model_version'] = self.model_version

        if task_id is not None:
            output['task'] = task_id

        return output

    @abstractmethod
    def to_dict(self) -> dict:
        ...

    @classmethod
    @abstractmethod
    def from_dict(cls, d: dict) -> 'BaseAnnotation':
        ...


class BaseTask(ABC):
    """Abstract base for a Label Studio task."""

    def __init__(self, annotations: list = None):
        self.annotations: list = annotations or []

    @abstractmethod
    def data_dict(self) -> dict:
        """Subclasses return their custom data fields as a dict."""
        ...

    def as_new_taskdata_dict(self, predictions_key: str = None,
                             annotations_key: str = None) -> dict:
        """Build a complete task dict for upload to Label Studio."""
        task = {"data": self.data_dict()}
        if predictions_key:
            preds = [a.as_dict() for a in self.annotations if a.is_prediction()]
            if preds:
                task[predictions_key] = preds
        if annotations_key:
            annots = [a.as_dict() for a in self.annotations if not a.is_prediction()]
            if annots:
                task[annotations_key] = annots
        return task

    @abstractmethod
    def to_dict(self) -> dict:
        ...

    @classmethod
    @abstractmethod
    def from_dict(cls, d: dict) -> 'BaseTask':
        ...
